//! Support for capturing stack backtraces
//!
//! This module contains the support for capturing a stack backtrace through
//! the [`Backtrace`] type. Backtraces are helpful to attach to errors,
//! containing information that can be used to get a chain of where an error
//! was created.

use alloc::vec::Vec;
use core::{ffi::c_void, fmt::Display};

#[cfg(target_arch = "arm")]
use crate::unwind::*;

/// A captured stack backtrace.
///
/// This type stores the backtrace of a captured stack at a certain point in
/// time. The backtrace is represented as a list of instruction pointers.
///
/// ```
/// let backtrace = Backtrace::capture();
/// println!("{backtrace}");
/// ```
///
/// ## Symbolication
///
/// The number stored in each frame is not particularly meaningful to humans on its own.
/// Using a tool such as `llvm-symbolizer` or `addr2line`, it can be turned into
/// a function name and line number to show what functions were being run at
/// the time of the backtrace's capture.
///
/// ```terminal
/// $ llvm-symbolizer -p -e ./target/armv7a-vex-v5/debug/program_name 0x380217b 0x380209b
/// my_function at /path/to/project/src/main.rs:30:14
///
/// main at /path/to/project/src/main.rs:21:9
/// ```
///
/// ## Platform Support
///
/// WebAssembly platforms are not supported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backtrace {
    /// The instruction pointers of each frame in the backtrace.
    pub frames: Vec<*const c_void>,
}

impl Backtrace {
    /// Captures a backtrace at the current point of execution.
    ///
    /// If a backtrace could not be captured, an empty backtrace is returned.
    #[inline(always)] // Inlining keeps this function from appearing in backtraces
    #[allow(clippy::missing_const_for_fn)]
    pub fn capture() -> Self {
        #[cfg(target_arch = "arm")]
        return Self::try_capture().unwrap_or(Self { frames: Vec::new() });

        #[cfg(target_arch = "wasm32")]
        return Self { frames: Vec::new() };
    }

    /// Captures a backtrace at the current point of execution,
    /// returning an error if the backtrace fails to capture.
    #[inline(never)] // Make sure there's alawys a frame to remove
    #[cfg(target_arch = "arm")]
    pub fn try_capture() -> Result<Self, UnwindError> {
        let mut context = UnwindContext::new()?;
        let mut cursor = UnwindCursor::new(&mut context)?;

        let mut frames = Vec::new();

        // Procedure based on mini_backtrace crate.

        while cursor.step() {
            let mut instruction_pointer = cursor.get_register(sys::UNW_REG_IP)?;

            // Adjust the IP to point within the function symbol. This should
            // only be done if the frame is not a signal frame.
            if !cursor.is_signal_frame() {
                instruction_pointer -= 1;
            }

            frames.push(instruction_pointer as *const c_void);
        }

        Ok(Self { frames })
    }
}

impl Display for Backtrace {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        writeln!(f, "stack backtrace:")?;
        for (i, frame) in self.frames.iter().enumerate() {
            writeln!(f, "{i:>3}: {:?}", frame)?;
        }
        write!(
            f,
            "note: Use a symbolizer to convert stack frames to human-readable function names."
        )?;
        Ok(())
    }
}

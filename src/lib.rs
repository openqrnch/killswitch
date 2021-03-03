//! Signal a request for (multiple) async tasks to self-terminate.
//!
//! ```
//! use std::error::Error;
//! use tokio::time::{sleep, Duration};
//! use killswitch::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!   let (killswitch, shutdown) = killswitch();
//!
//!   tokio::spawn(croakable(String::from("test1"), shutdown.clone()));
//!   tokio::spawn(croakable(String::from("test2"), shutdown.clone()));
//!
//!   sleep(Duration::from_secs(1)).await;
//!
//!   println!("Triggering kill switch");
//!   killswitch.trigger();
//!
//!   tokio::spawn(croakable(String::from("test3"), shutdown.clone()));
//!   tokio::spawn(croakable(String::from("test4"), shutdown));
//!
//!   // Wait for all associated shutdown objects to become inactive.
//!   killswitch.await;
//!
//!   Ok(())
//! }
//!
//! async fn croakable(s: String, shutdown: Shutdown) {
//!   println!("croakable({}) entered", s);
//!   shutdown.wait().await;
//!   println!("croakable({}) leaving", s);
//! }
//! ```
//!
//! `killswitch` was developed to help create abortable async tasks by using
//! multiple-wait features such as the `tokio::select!` macro.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};


/// Shared state buffer that is hidden behind a Mutex.
struct State {
  /// Once the killswitch is triggered there needs to be a way to wake all
  /// tasking waiting for a shutdown.  This map is used to keep track of all
  /// active wakers.
  waiting: HashMap<usize, Waker>,

  /// Waker used for waiting for all Shutdown objects to be released.
  waker: Option<Waker>
}

/// Buffer shared among all KillSwitch and Shutdown objects.
struct Shared {
  /// Each Shutdown+Waker is assigned a unique id.  This value is used as a
  /// monotonically increasing value used to generate those id:s.
  /// In theory there's a wrap-around problem.  In practice this will never
  /// happen.
  id: AtomicUsize,

  /// Keep track of whether the killswitch has been triggered or not.
  shutdown: AtomicBool,

  state: Mutex<State>
}


/// The kill switch used to signal that tasks should self-terminate.
pub struct KillSwitch {
  ctx: Arc<Shared>
}

impl KillSwitch {
  /// Signal all associated [`Shutdown`] objects that their tasks should
  /// terminate.
  pub fn trigger(&self) {
    // Mark kill switch as "set".
    self.ctx.shutdown.store(true, Ordering::SeqCst);

    // Tell all waiting tasks to wake up [and check the kill switch].
    let mut state = self.ctx.state.lock().unwrap();
    for (_, waker) in state.waiting.drain() {
      waker.wake();
    }
  }
}

impl Future for KillSwitch {
  type Output = ();
  fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
    let mut state = self.ctx.state.lock().unwrap();
    if state.waiting.is_empty() {
      Poll::Ready(())
    } else {
      state.waker = Some(ctx.waker().clone());
      Poll::Pending
    }
  }
}


/// Receiver object for kill switches.
pub struct Shutdown {
  ctx: Arc<Shared>
}

impl Shutdown {
  /// Return a future which waits for the kill switch to trigger.
  pub fn wait(&self) -> ShutdownFuture {
    ShutdownFuture {
      ctx: Arc::clone(&self.ctx),
      id: Arc::new(AtomicUsize::new(0))
    }
  }
}

impl Clone for Shutdown {
  fn clone(&self) -> Shutdown {
    Shutdown {
      ctx: Arc::clone(&self.ctx)
    }
  }
}


/// Future returned by [`Shutdown::wait()`].
pub struct ShutdownFuture {
  ctx: Arc<Shared>,
  id: Arc<AtomicUsize>
}

impl Future for ShutdownFuture {
  type Output = ();
  fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.ctx.shutdown.load(Ordering::SeqCst) {
      true => Poll::Ready(()),
      false => {
        // ToDo: Ordering

        // Generate a new id (make sure it isn't zero).
        let id = loop {
          let id = self.ctx.id.fetch_add(1, Ordering::SeqCst);
          if id != 0 {
            break id;
          }
        };

        // If the current id is 0, then assign it the newly generated id and
        // add us to the wakers.
        if let Ok(_) =
          self
            .id
            .compare_exchange(0, id, Ordering::SeqCst, Ordering::SeqCst)
        {
          let mut state = self.ctx.state.lock().unwrap();
          state.waiting.insert(id, ctx.waker().clone());
        }

        Poll::Pending
      }
    }
  }
}


/// If a Shutdown object is released and it has a Waker in the internal
/// hashmap, then release that Waker object.
impl Drop for ShutdownFuture {
  fn drop(&mut self) {
    let id = self.id.load(Ordering::SeqCst);
    if id != 0 {
      let mut state = self.ctx.state.lock().unwrap();
      state.waiting.remove(&id);

      // If the hashmap is empty and there's a killswitch waker stored in the
      // state, then signal it.
      if state.waiting.is_empty() {
        let old = std::mem::replace(&mut state.waker, None);
        if let Some(waker) = old {
          waker.wake();
        }
      }
    }
  }
}


/// Create a connected [`KillSwitch`] and a [`Shutdown`] pair.
pub fn killswitch() -> (KillSwitch, Shutdown) {
  let state = State {
    waiting: HashMap::new(),
    waker: None
  };
  let shared = Shared {
    id: AtomicUsize::new(1),
    shutdown: AtomicBool::new(false),
    state: Mutex::new(state)
  };
  let shared = Arc::new(shared);

  let killswitch = KillSwitch {
    ctx: Arc::clone(&shared)
  };
  let shutdown = Shutdown { ctx: shared };

  (killswitch, shutdown)
}

// vim: set ft=rust et sw=2 ts=2 sts=2 cinoptions=2 tw=79 :

// Copyright 2020-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Where a subscription delivers its messages, and the handle of one that delivers into a
//! channel the caller owns.

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

use crate::{Command, Message, UnsubscribeError};

/// Takes each message of a subscription made with [crate::Client::subscribe_with_route] on the
/// connection task, as it is read.
///
/// `deliver` runs inside the task that reads and writes the connection, so it must never block
/// and should only move the message somewhere and wake whoever reads it. A route lives until its
/// [`SubscriptionHandle`] or the client drops, or until it answers [`Routed::Closed`], and the
/// connection drops it then.
pub trait Route: Send + Sync + 'static {
    /// Takes one message without blocking.
    fn deliver(&mut self, message: Message) -> Routed;
}

/// What a [`Route`] did with one message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Routed {
    /// The route took the message.
    Taken,
    /// The route had no room and dropped the message, which raises a slow consumer event.
    Full,
    /// The route wants no more messages, which unsubscribes it.
    Closed,
}

/// Where a subscription's messages go: its own channel, a caller's channel tagged by sid, or a
/// caller's route.
pub(crate) enum Delivery {
    Own(mpsc::Sender<Message>),
    Shared(mpsc::Sender<(u64, Message)>),
    Routed(Box<dyn Route>),
}

impl std::fmt::Debug for Delivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Delivery::Own(sender) => f.debug_tuple("Own").field(sender).finish(),
            Delivery::Shared(sender) => f.debug_tuple("Shared").field(sender).finish(),
            Delivery::Routed(_) => f.write_str("Routed"),
        }
    }
}

impl Delivery {
    pub(crate) fn try_send(&mut self, sid: u64, message: Message) -> Result<(), TrySendError<()>> {
        match self {
            Delivery::Own(sender) => sender.try_send(message).map_err(forget),
            Delivery::Shared(sender) => sender.try_send((sid, message)).map_err(forget),
            Delivery::Routed(route) => match route.deliver(message) {
                Routed::Taken => Ok(()),
                Routed::Full => Err(TrySendError::Full(())),
                Routed::Closed => Err(TrySendError::Closed(())),
            },
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Delivery::Own(sender) => sender.is_closed(),
            Delivery::Shared(sender) => sender.is_closed(),
            Delivery::Routed(_) => false,
        }
    }
}

fn forget<T>(error: TrySendError<T>) -> TrySendError<()> {
    match error {
        TrySendError::Full(_) => TrySendError::Full(()),
        TrySendError::Closed(_) => TrySendError::Closed(()),
    }
}

/// A subscription made with [crate::Client::subscribe_with_sender], which unsubscribes on drop.
#[derive(Debug)]
pub struct SubscriptionHandle {
    sid: u64,
    sender: mpsc::Sender<Command>,
    subscribed: bool,
}

impl SubscriptionHandle {
    pub(crate) fn new(sid: u64, sender: mpsc::Sender<Command>) -> SubscriptionHandle {
        SubscriptionHandle {
            sid,
            sender,
            subscribed: true,
        }
    }

    /// The subscription id that tags this subscription's messages.
    pub fn sid(&self) -> u64 {
        self.sid
    }

    /// Unsubscribes from the subject.
    ///
    /// This method is the reliable unsubscribe path: it waits for the command to be accepted by
    /// the client's command channel. Dropping [`SubscriptionHandle`] is best-effort.
    ///
    /// # Examples
    /// ```no_run
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), async_nats::Error> {
    /// let client = async_nats::connect("demo.nats.io").await?;
    /// let (sender, _receiver) = tokio::sync::mpsc::channel(16);
    /// let handle = client.subscribe_with_sender("foo", sender).await?;
    /// handle.unsubscribe().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn unsubscribe(mut self) -> Result<(), UnsubscribeError> {
        let sid = self.sid;
        self.sender
            .send(Command::Unsubscribe { sid, max: None })
            .await?;
        // Cleared only once the command is queued, so a cancelled call still unsubscribes on drop.
        self.subscribed = false;
        Ok(())
    }
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        if self.subscribed {
            crate::unsubscribe_on_drop(&self.sender, self.sid, "SubscriptionHandle");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures_util::FutureExt;
    use tokio::sync::mpsc;

    use super::SubscriptionHandle;
    use crate::Command;

    async fn next_unsubscribed(commands: &mut mpsc::Receiver<Command>) -> u64 {
        let command = tokio::time::timeout(Duration::from_secs(5), commands.recv()).await;
        match command.expect("a command arrives") {
            Some(Command::Unsubscribe { sid, max: None }) => sid,
            other => panic!("unexpected command {other:?}"),
        }
    }

    #[tokio::test]
    async fn unsubscribe_queues_one_command() {
        let (sender, mut commands) = mpsc::channel(4);
        let handle = SubscriptionHandle::new(2, sender);

        handle.unsubscribe().await.unwrap();

        assert_eq!(next_unsubscribed(&mut commands).await, 2);
        // The handle held the only sender, so the channel ends with nothing more queued.
        assert!(commands.recv().await.is_none());
    }

    #[tokio::test]
    async fn a_cancelled_unsubscribe_still_unsubscribes_on_drop() {
        let (sender, mut commands) = mpsc::channel(1);
        let filler = Command::Unsubscribe { sid: 1, max: None };
        sender.send(filler).await.unwrap();
        let handle = SubscriptionHandle::new(2, sender);

        // The command channel is full, so the send is still pending when the call is dropped.
        assert!(handle.unsubscribe().now_or_never().is_none());

        assert_eq!(next_unsubscribed(&mut commands).await, 1);
        assert_eq!(next_unsubscribed(&mut commands).await, 2);
    }
}

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

/// Where a subscription's messages go: its own channel, or a caller's channel tagged by sid.
#[derive(Debug)]
pub(crate) enum Delivery {
    Own(mpsc::Sender<Message>),
    Shared(mpsc::Sender<(u64, Message)>),
}

impl Delivery {
    pub(crate) fn try_send(&self, sid: u64, message: Message) -> Result<(), TrySendError<()>> {
        match self {
            Delivery::Own(sender) => sender.try_send(message).map_err(forget),
            Delivery::Shared(sender) => sender.try_send((sid, message)).map_err(forget),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        match self {
            Delivery::Own(sender) => sender.is_closed(),
            Delivery::Shared(sender) => sender.is_closed(),
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
        self.subscribed = false;
        let sid = self.sid;
        self.sender
            .send(Command::Unsubscribe { sid, max: None })
            .await?;
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

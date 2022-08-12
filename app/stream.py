# TODO: Add logger

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from db import database_connection, write_to_db


EVENTS_CHANNEL_NAME = "pubnub-market-orders"


def pubnub_config():
    pnconfig = PNConfiguration()
    # pnconfig.daemon = True  # spawned thread won't keep the application running after SIGTERM. (ctrl-c from command line, for example)
    pnconfig.subscribe_request_timeout = 10
    pnconfig.subscribe_key = "sub-c-4377ab04-f100-11e3-bffd-02ee2ddab7fe"
    pnconfig.subscribe_callback = SubscribeCallback
    pnconfig.user_id = "market-order-app"
    return pnconfig


class MarketOrderStreamSubscribeCallback(SubscribeCallback):
    def status(self, pubnub, status):
        print("Status category", status.category)
        if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
            print("Disconnect category", status.category)  # This event happens when connectivity is lost
            # TODO: Log this event

        elif status.category == PNStatusCategory.PNConnectedCategory:
            # Connect event. You can do stuff like publish, and know you'll get it.
            # Or just use the connected event to confirm you are subscribed for
            # UI / internal notifications, etc
            # TODO: Log this event
            print("Connected category/Subscribe to", status.category)

    def message(self, pubnub, message):
        # Handle new message stored in message.message
        print("Message payload: %s" % message.message)
        # TODO: Call database insert method - write_to_db
        # pubnub.unsubscribe().channels("someChannel").execute()


pubnub = PubNub(pubnub_config())
pubnub.add_listener(MarketOrderStreamSubscribeCallback())
pubnub.subscribe().channels(EVENTS_CHANNEL_NAME).execute()

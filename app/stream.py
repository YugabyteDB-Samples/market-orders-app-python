# TODO: Add logger

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from db import database_connection, write_trade_to_db
import random


EVENTS_CHANNEL_NAME = "pubnub-market-orders"
conn = database_connection()
conn.set_session(autocommit=True)
cur = conn.cursor()


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
        if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
            print("Disconnect category", status.category)  # This event happens when connectivity is lost
            # TODO: Log this event

        elif status.category == PNStatusCategory.PNConnectedCategory:
            # Connect event. You can do stuff like publish, and know you'll get it.
            # Or just use the connected event to confirm you are subscribed for
            # UI / internal notifications, etc
            # TODO: Log this event
            subscribe_message = f"Connected category/Subscribe to {EVENTS_CHANNEL_NAME} channel"
            print(subscribe_message)

    def message(self, pubnub, message):
        # Handle new message stored in message.message
        print("Message payload: %s" % message.message)
        cur.execute('select id from public."User";')
        user_ids = [record[0] for record in cur.fetchall()]
        user_id = random.choice(user_ids)
        write_trade_to_db(conn, message.message, user_id)


if __name__ == '__main__':
    pubnub = PubNub(pubnub_config())
pubnub.add_listener(MarketOrderStreamSubscribeCallback())
pubnub.subscribe().channels(EVENTS_CHANNEL_NAME).execute()

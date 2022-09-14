"""Flask app for PubNub Market Order Stream"""

import logging
import threading

import pubnub as pn
from flask import Flask, jsonify, render_template, request
from pubnub import utils
from pubnub.pubnub import PubNub

from db import database_connection
from pubnub_stream_utils import (
    DEFAULT_EVENTS_CHANNEL_NAME,
    ingest_pubnub_stream_data,
    pubnub_config,
)

pn.set_stream_logger("pubnub", logging.DEBUG)
logger = logging.getLogger("myapp")

app = Flask(__name__)
APP_KEY = utils.uuid()

pubnub = PubNub(pubnub_config())
logger.info("SDK Version: %s", pubnub.SDK_VERSION)


@app.route("/")
@app.route("/index")
def home():
    """Returns the app home page"""
    return render_template(
        "index.html", title="Home page", channel=DEFAULT_EVENTS_CHANNEL_NAME
    )


@app.route("/app_key")
def app_key():
    """Returns the unique app key"""
    return jsonify({"app_key": APP_KEY}), 200


@app.route("/subscription/add")
def subscription_add():
    """Adds a new subscription"""
    channel = request.args.get("channel")
    if channel is None:
        return jsonify({"error": "Channel missing"}), 500
    pubnub.subscribe().channels(channel).execute()
    return jsonify({"subscribed_channels": pubnub.get_subscribed_channels()}), 200


@app.route("/subscription/remove")
def subscription_remove():
    """Removes a subscription"""
    channel = request.args.get("channel")
    if channel is None:
        return jsonify({"error": "Channel missing"}), 500
    pubnub.unsubscribe().channels(channel).execute()
    return jsonify({"removed subscribed channel": channel}), 200


@app.route("/subscription/list")
def subscription_list():
    """Returns a list of all subscriptions"""
    return jsonify({"subscribed_channels": pubnub.get_subscribed_channels()}), 200


@app.route("/ingeststreamdata")
def ingest_stream_data():
    """Ingests trade data from pubnub stream and writes to database as per channel"""
    channel = request.args.get("channel")
    if channel is None:
        return jsonify({"error": "Channel missing"}), 500
    thread = threading.Thread(
        target=ingest_pubnub_stream_data,
        kwargs={"channel": channel, "pubnub_obj": pubnub},
    )
    thread.start()
    return jsonify({"message": "Request accepted"}), 202


@app.route("/tradestats")
def get_trade_stats():
    """Returns trade stats"""
    conn = database_connection()
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    top_symbol_query = (
        'select symbol from public."Trade" group by symbol order by count(*) desc;'
    )
    cur.execute(top_symbol_query)
    top_symbol = cur.fetchone()[0]

    top_buyer_query = "select first_name, last_name, total_portfolio_value::float  from top_buyers_view tbv  where  total_portfolio_value  = (select max(total_portfolio_value) from top_buyers_view tbv2);"
    cur.execute(top_buyer_query)
    top_buyer_query_res = cur.fetchall()
    top_buyer = dict()
    top_buyer["first_name"] = top_buyer_query_res[0][0]
    top_buyer["last_name"] = top_buyer_query_res[0][1]
    top_buyer["total_portfolio_value"] = top_buyer_query_res[0][2]

    total_trade_count_query = 'select count(id) from public."Trade" t;'
    cur.execute(total_trade_count_query)
    total_trade_count = cur.fetchone()[0]

    return (
        jsonify(
            {
                "total_trade_count": total_trade_count,
                "top_buyer": top_buyer,
                "top_symbol": top_symbol,
            }
        ),
        200,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5001")

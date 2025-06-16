#!python3
import time
import traceback
from requests.exceptions import ConnectionError, ReadTimeout, RequestException
from binance.exceptions import BinanceAPIException

from .binance_api_manager import BinanceAPIManager
from .config import Config
from .database import Database
from .logger import Logger
from .scheduler import SafeScheduler
from .strategies import get_strategy


def main():
    logger = Logger()
    logger.info("Starting")

    config = Config()
    db = Database(logger, config)
    manager = BinanceAPIManager(config, db, logger, config.TESTNET)
    # check if we can access API feature that require valid config
    try:
        _ = manager.get_account()
    except Exception as e:  # pylint: disable=broad-except
        logger.error("Couldn't access Binance API - API keys may be wrong or lack sufficient permissions")
        logger.error(e)
        return
    strategy = get_strategy(config.STRATEGY)
    if strategy is None:
        logger.error("Invalid strategy name")
        return
    trader = strategy(manager, db, logger, config)
    logger.info(f"Chosen strategy: {config.STRATEGY}")

    logger.info("Creating database schema if it doesn't already exist")
    db.create_database()

    db.set_coins(config.SUPPORTED_COIN_LIST)
    db.migrate_old_state()

    trader.initialize()

    schedule = SafeScheduler(logger)
    schedule.every(config.SCOUT_SLEEP_TIME).seconds.do(trader.scout).tag("scouting")
    schedule.every(1).minutes.do(trader.update_values).tag("updating value history")
    schedule.every(1).minutes.do(db.prune_scout_history).tag("pruning scout history")
    schedule.every(1).hours.do(db.prune_value_history).tag("pruning value history")

    consecutive_errors = 0
    max_consecutive_errors = 5
    error_cooldown = 60  # seconds to wait after multiple errors

    try:
        while True:
            try:
                schedule.run_pending()
                consecutive_errors = 0  # Reset error counter on successful run
                time.sleep(1)
            except (ConnectionError, ReadTimeout, RequestException, BinanceAPIException) as e:
                consecutive_errors += 1
                logger.error(f"Connection error occurred: {str(e)}")
                logger.error(traceback.format_exc())
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.warning(f"Too many consecutive errors ({consecutive_errors}). Waiting {error_cooldown} seconds before retrying...")
                    time.sleep(error_cooldown)
                    consecutive_errors = 0
                else:
                    time.sleep(5)  # Short delay before retrying
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f"Unexpected error occurred: {str(e)}")
                logger.error(traceback.format_exc())
                time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        manager.stream_manager.close()
        logger.info("Shutdown complete")
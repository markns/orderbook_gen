import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from random import randint, random

import timesynth as ts
import numpy as np
import redis
import time

r = redis.StrictRedis(host='localhost', port=6379, db=0)

start_time = 1536292800 + (60 * 60 * 4)

emini_spread = 0.25  # Outright: 0.25 index points=$12.50
eurodollar_spread = 0.01  # 0.005 price points = $12.50 per contract
crude_spread = 0.01  # $0.01 per Barrel


class Instrument:
    def __init__(self, code, expiration: datetime, spread, update_freq):
        self.code = code
        self.expiration = expiration
        self.spread = spread
        self.update_freq = update_freq


def x_round(x):
    return round(x * 4) / 4


class OrderBookSim:
    def __init__(self, inst, mb_gen, settle, open, base_offset, trade_probability, size):
        self.instrument = inst
        self.mb_gen = mb_gen
        self.settle = settle
        self.base_offset = base_offset
        self.trade_probability = trade_probability
        self.size = size

        self.open = open + base_offset
        self.last = None
        self.high = None
        self.low = None
        self.volume = 0

    def gen(self, base, t):
        base += self.base_offset

        bid = x_round(base)
        ask = bid + self.instrument.spread

        traded = randint(1, self.trade_probability)
        if traded < 3:
            side = 'buy'
            if traded == 1:
                self.last = bid
            elif traded == 2:
                side = 'sell'
                self.last = ask
            size = randint(*self.size)
            self.volume += size

            d = dict(instrument=self.instrument.code, price=self.last, timestamp=start_time + t, size=size, side=side)
            r.publish('trades', json.dumps(d))
            self.mb_gen.update(d)

        change = 0
        if self.last:
            if not self.high:
                self.high = self.last
                self.low = self.last
            else:
                if self.last > self.high:
                    self.high = self.last
                if self.last < self.low:
                    self.low = self.last
            change = self.last - self.settle

        update = dict(expiration=self.instrument.expiration.timestamp(),
                      last=self.last,
                      change=change,
                      settle=self.settle,
                      open=self.open,
                      bid=bid,
                      ask=ask,
                      high=self.high,
                      low=self.low,
                      volume=self.volume,
                      updated=start_time + t)

        r.publish(self.instrument.code, json.dumps(update))


class MinuteBarGenerator:
    def __init__(self):
        self.curr_min = None
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.volume = None

    def new_min(self, dt, data):
        self.curr_min = datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute)
        self.open = data['price']
        self.high = data['price']
        self.low = data['price']
        self.close = data['price']
        self.volume = 0

    def update(self, data):
        dt = datetime.fromtimestamp(data['timestamp'])

        if not data['price']:
            return

        if not self.curr_min:
            self.new_min(dt, data)
            return

        if dt.minute == self.curr_min.minute:
            if data['price'] > self.high:
                self.high = data['price']
            if data['price'] < self.low:
                self.low = data['price']

            self.close = data['price']
            self.volume += data['size']

        else:
            ohlc = dict(timestamp=self.curr_min.timestamp(), open=self.open, high=self.high,
                        low=self.low, close=self.close, volume=self.volume)
            r.rpush(data['instrument'], json.dumps(ohlc))

            self.new_min(dt, data)




def run_sim(obsims, time_samples, samples):

    for sim in obsims:
        r.delete([sim.instrument.code])

    initializing = True

    for d in zip(time_samples, np.diff(time_samples), samples[0]):
        t, sleep, sample = d

        if t > 3600:
            if initializing:
                print("done initializing")
            initializing = False
            time.sleep(sleep)

        for sim in obsims:
            instrument = sim.instrument
            if random() < instrument.update_freq:
                sim.gen(sample, t)




def es():
    open = 2873

    # Initializing TimeSampler
    time_sampler = ts.TimeSampler(stop_time=7200)
    # # Sampling irregular time samples
    irregular_time_samples = time_sampler.sample_irregular_time(num_points=7200, keep_percentage=30)

    car = ts.signals.CAR(ar_param=0.9, sigma=3)
    car_series = ts.TimeSeries(signal_generator=car)
    samples = car_series.sample(irregular_time_samples)
    samples[0][:] += open

    obsims = [
        OrderBookSim(Instrument('ESU18', datetime(2018, 9, 21), emini_spread, 1), MinuteBarGenerator(),
                     2879, open, 0, 10, (10, 1000)),
        OrderBookSim(Instrument('ESZ18', datetime(2018, 12, 21), emini_spread, 0.8), MinuteBarGenerator(),
                     2883.75, open, 4.75, 20, (10, 100)),
        OrderBookSim(Instrument('ESH18', datetime(2019, 3, 15), emini_spread, 0.7), MinuteBarGenerator(),
                     2890.75, open, 11.75, 25, (5, 50)),
        OrderBookSim(Instrument('ESM18', datetime(2019, 6, 21), emini_spread, 0.5), MinuteBarGenerator(),
                     2899, open, 20, 30, (1, 10)),
        OrderBookSim(Instrument('ESU19', datetime(2019, 9, 20), emini_spread, 0.3), MinuteBarGenerator(),
                     2902, open, 23, 40, (1, 10))
    ]

    run_sim(obsims, irregular_time_samples, samples)


def eurodollar():
    open = 97.6425

    # Initializing TimeSampler
    time_sampler = ts.TimeSampler(stop_time=7200)
    # # Sampling irregular time samples
    irregular_time_samples = time_sampler.sample_irregular_time(num_points=7200, keep_percentage=30)

    car = ts.signals.CAR(ar_param=0.9, sigma=3)
    car_series = ts.TimeSeries(signal_generator=car)
    samples = car_series.sample(irregular_time_samples)
    samples[0][:] += open

    obsims = [
        OrderBookSim(Instrument('EDU18', datetime(2018, 9, 17), eurodollar_spread, 1), MinuteBarGenerator(),
                     97.6425, open, 0, 10, (10, 1000)),
        OrderBookSim(Instrument('EDZ18', datetime(2018, 12, 17), eurodollar_spread, 0.8), MinuteBarGenerator(),
                     97.345, open, -0.3, 20, (10, 100)),
        OrderBookSim(Instrument('EDH18', datetime(2019, 3, 18), eurodollar_spread, 0.7), MinuteBarGenerator(),
                     97.185, open, -0.5, 25, (5, 50)),
        OrderBookSim(Instrument('EDM18', datetime(2019, 6, 17), eurodollar_spread, 0.5), MinuteBarGenerator(),
                     97.06, open, -0.7, 30, (1, 10)),
        OrderBookSim(Instrument('EDU19', datetime(2019, 9, 16), eurodollar_spread, 0.3), MinuteBarGenerator(),
                     97.00, open, -0.9, 40, (1, 10))
    ]

    run_sim(obsims, irregular_time_samples, samples)


def crude():
    open = 67.88

    # Initializing TimeSampler
    time_sampler = ts.TimeSampler(stop_time=7200)
    # # Sampling irregular time samples
    irregular_time_samples = time_sampler.sample_irregular_time(num_points=7200, keep_percentage=30)

    car = ts.signals.CAR(ar_param=0.9, sigma=3)
    car_series = ts.TimeSeries(signal_generator=car)
    samples = car_series.sample(irregular_time_samples)
    samples[0][:] += open

    obsims = [
        OrderBookSim(Instrument('CLV18', datetime(2018, 9, 20), crude_spread, 1), MinuteBarGenerator(),
                     67.88, open, 0, 10, (10, 1000)),
        OrderBookSim(Instrument('CLX18', datetime(2018, 10, 22), crude_spread, 0.8), MinuteBarGenerator(),
                     67.72, open, -.1, 20, (10, 100)),
        OrderBookSim(Instrument('CLZ18', datetime(2018, 11, 19), crude_spread, 0.7), MinuteBarGenerator(),
                     67.63, open, -.25, 25, (5, 50)),
        OrderBookSim(Instrument('CLF19', datetime(2018, 12, 19), crude_spread, 0.5), MinuteBarGenerator(),
                     67.44, open, -.4, 30, (1, 10)),
        OrderBookSim(Instrument('CLG19', datetime(2019, 1, 22), crude_spread, 0.3), MinuteBarGenerator(),
                     67.28, open, -.6, 40, (1, 10))
    ]

    run_sim(obsims, irregular_time_samples, samples)


if __name__ == "__main__":
    r.delete('trades')
    executor = ThreadPoolExecutor(max_workers=3)
    a = executor.submit(es)
    b = executor.submit(eurodollar)
    c = executor.submit(crude)

    for future in as_completed([a, b, c]):
        try:
            data = future.result()
        except Exception as exc:
            print(exc)

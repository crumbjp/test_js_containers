const _ = require('lodash');

function generateNormalDistribution(mean, stdDev, size, minValue, maxValue, round) {
  return _.chain(Array(size))
    .map(_elem => {
      const randStdNormal = Math.sqrt(-2.0 * Math.log(Math.random())) * Math.sin(2.0 * Math.PI * Math.random());
      const value = mean + stdDev * randStdNormal;
      if (value >= minValue && value <= maxValue) {
        if (_.isNumber(round)) {
          return _.round(value, round);
        }
        return value;
      }
      return null;
    })
    .compact()
    .value();
}

function generateData(n, offset, stdDev) {
  return Array.from({length: n}, () => Math.random() * stdDev + offset - (stdDev / 2));
}

const ROUND = 4;
const MIN = 0;
const MAX = 10;

function generateSeeds(times) {
  return generateNormalDistribution(1, 1, 500*times, MIN, MAX, ROUND)
    .concat(generateNormalDistribution(5, 5, 200*times, MIN, MAX, ROUND))
    .concat(generateNormalDistribution(10, 1, 1000*times, MIN, MAX, ROUND));
}
const points = generateNormalDistribution(5, 1, 1000, MIN, MAX, ROUND);

class Test {
  constructor(seeds, points) {
    console.log(`===== ${this.constructor.name} =====`)
    this.seeds = seeds;
    this.points = points;
  }

  log (stage, ms, suffix = '') {
    this._log = this._log || {name: this.constructor.name};
    this._log[stage] = ms;
    console.log(` - ${stage} ${ms} ms ${suffix}`);
  }

  async prepare() {
    {
      const start = new Date().getTime();
      await this._prepare();
      const end = new Date().getTime();
      this.log('prepare()', end - start, `size: ${await this.size()}`);
    }
  }

  async bulkInsert() {
    const start = new Date().getTime();
    await this._bulkInsert(_.map(this.seeds, (p) => ({p})));
    const end = new Date().getTime();
    this.log('bulk()', end - start, `size: ${await this.size()}`);
  }

  async insert() {
    const start = new Date().getTime();
    for (const p of this.points) {
      await this._insert({p});
    }
    const end = new Date().getTime();
    this.log('insert()', end - start, `size: ${await this.size()}`);
  }

  async remove() {
    const start = new Date().getTime();
    for (const p of this.points) {
      await this._remove(p);
    }
    const end = new Date().getTime();
    this.log('remove()', end - start, `size: ${await this.size()}`);
  }

  async findOne() {
    const start = new Date().getTime();
    for (const p of this.points) {
      await this._findOne(p);
    }
    const end = new Date().getTime();
    this.log('findOne()', end - start, `size: ${await this.size()} sample: ${JSON.stringify(await this._findOne(this.points[0]))}`);
  }

  async range() {
    const start = new Date().getTime();
    const results = await this._range(4, 6);
    const end = new Date().getTime();
    this.log('range()', end - start, `size: ${await this.size()} len: ${results.length} sample: ${JSON.stringify(JSON.stringify(results[0]))}`);
  }

  async test() {
    try {
      global.gc();
      await this.prepare();
      await this.bulkInsert();
      await this.insert();
      await this.findOne();
      await this.range();
      await this.remove();
      return this._log;
    } catch (e) {
      console.log(e);
      return {name: this.constructor.name};
    }
  }
}

class TestArray extends Test {
  constructor(seeds, points) {
    super(seeds, points);
  }

  async _prepare() {
    this.container = [];
  }

  async _bulkInsert(objs) {
    this.container = this.container.concat(objs);
    // Array.prototype.push.apply(this.container, objs); // Overusing call stack
  }

  async size() {
    return this.container.length;
  }

  async _insert(obj) {
    this.container.push(obj)
  }

  async _findOne(p) {
    const index = this.container.findIndex((obj) => obj.p == p);
    return this.container[index];
  }

  async _range(from, to) {
    return _.chain(this.container)
      .filter((obj) => (obj.p >= from && obj.p <= to))
      .sort((a, b) => a.p - b.p)
      .value();
  }

  async _remove(p) {
    const index = this._findOne(p);
    this.container.splice(index, 1);
  }
}

class TestLoki extends Test {
  constructor(seeds, points) {
    super(seeds, points);
  }

  async _prepare(objs) {
    const Loki = require('lokijs')
    this.db = new Loki('test.db');
    this.collection = this.db.addCollection(
      'samples', {
        indices: ['p'],
        // adaptiveBinaryIndices: false, // Slow fatally
        disableMeta: true,
      });
  }

  async _bulkInsert(objs) {
    this.collection.insert(objs);
  }

  async size() {
    return this.collection.count();
  }

  async _insert(obj) {
    this.collection.insert(obj);
  }

  async _findOne(p) {
    return this.collection.findOne({p});
  }

  async _range(from, to) {
    // Dont work
    // return this.collection.find({
    //   p: {
    //     $gte: from,
    //     $lte: to,
    //   }
    // });
    return this.collection.find({
      p: {
        $between: [from, to],
      }
    });

  }

  async _remove(p) {
    const doc = await this._findOne(p)
    return this.collection.remove(doc);
  }
}

class TestNedb extends Test {
  constructor(seeds, points) {
    super(seeds, points);
  }

  _prepare(objs) {
    const Nedb = require('nedb')
    this.db = new Nedb();
    return new Promise((resolve, reject) => {
      this.db.ensureIndex({ fieldName: 'p'}, (err) => {
        if (err) {
          return reject(err);
        }
        resolve();
      });
    });
  }

  _bulkInsert(objs) {
    return new Promise((resolve, reject) => {
      this.db.insert(objs, (err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve();
      });
    });
  }

  size() {
    return new Promise((resolve, reject) => {
      this.db.count({}, (err, count) => {
        if (err) {
          return reject(err);
        }
        resolve(count);
      });
    });
  }

  _insert(obj) {
    return new Promise((resolve, reject) => {
      this.db.insert(obj, (err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve();
      });
    });
  }

  _findOne(p) {
    return new Promise((resolve, reject) => {
      this.db.findOne({p}, (err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve(doc);
      });
    });
  }

  _range(from, to) {
    return new Promise((resolve, reject) => {
      this.db.find({
        p: {
          $gte: from,
          $lte: to,
        },
      }, (err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve(doc);
      });
    });
  }

  _remove(p) {
    return new Promise((resolve, reject) => {
      this.db.remove({p}, {}, (err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve(doc);
      });
    });
  }
}

class TestTingodb extends Test {
  constructor(seeds, points) {
    super(seeds, points);
  }

  _prepare(objs) {
    const Tingodb = require('tingodb')({memStore: true}).Db;
    this.db = new Tingodb('', {});
    this.collection = this.db.collection(`test_${this.seeds.length}`);
    return new Promise((resolve, reject) => {
      this.collection.ensureIndex({ p: 1 }, (err) => {
        if (err) {
          return reject(err);
        }
        resolve();
      });
    });
  }

  _bulkInsert(objs) {
    return new Promise((resolve, reject) => {
      this.collection.insert(objs, (err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve();
      });
    });
  }

  size() {
    return new Promise((resolve, reject) => {
      this.collection.count({}, (err, count) => {
        if (err) {
          return reject(err);
        }
        resolve(count);
      });
    });
  }

  _insert(obj) {
    return new Promise((resolve, reject) => {
      this.collection.insert(obj, (err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve();
      });
    });
  }

  _findOne(p) {
    return new Promise((resolve, reject) => {
      this.collection.findOne({p}, (err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve(doc);
      });
    });
  }

  _range(from, to) {
    return new Promise((resolve, reject) => {
      this.collection.find({
        p: {
          $gte: from,
          $lte: to,
        },
      }).toArray((err, doc) => {
        if (err) {
          return reject(err);
        }
        resolve(doc);
      });
    });
  }

  _remove(p) {
    return new Promise((resolve, reject) => {
      this.collection.findOne({p}, {}, (err, doc) => {
        if (err) {
          return reject(err);
        }
        this.collection.remove({_id: doc._id}, {}, (err, res) => {
          if (err) {
            return reject(err);
          }
          resolve(res);
        });
      });
    });
  }
}

class TestTaffy extends Test {
  constructor(seeds, points) {
    super(seeds, points);
  }

  async _prepare(objs) {
    const Taffy = require('taffydb').taffy;
    this.db = Taffy([]);
  }

  async _bulkInsert(objs) {
    await this.db.insert(objs);
  }

  async size() {
    return await this.db().count();
  }

  async _insert(obj) {
    return await this.db.insert(obj);
  }

  async _findOne(p) {
    return await this.db({p}).get()[0];
  }

  async _range(from, to) {
    const docs = await this.db({
      p: {
        gte: from,
        lte: to,
      }
    }).get();
    return docs.sort((a, b) => a.p - b.p);
  }

  async _remove(p) {
    const doc = await this._findOne(p);
    return this.db({___id: doc.___id}).remove();
  }
}

class TestBTree extends Test {
  constructor(seeds, points) {
    super(seeds, points);
  }

  async _prepare() {
    this.uniqid = 1;
    const BTree = require('@tylerbu/sorted-btree-es6').BTree
    this.container = new BTree(undefined, (a, b) => {
      const diffP = a.p - b.p;
      if (diffP == 0 && a.id && b.id) {
        return a.id - b.id;
      }
      return diffP;
    });
  }

  async _bulkInsert(objs) {
    this.container.setRange(_.map(objs, obj => {
      return [{p: obj.p, id: this.uniqid++}, obj];
    }));
  }

  async size() {
    return this.container.size;
  }

  async _insert(obj) {
    return this.container.set({p: obj.p, id: this.uniqid++}, obj);
  }

  async _findOne(p) {
    return _.get(this.container.getRange({p: p}, {p: p}, true), '0.1');
  }

  async _range(from, to) {
    return _.map(this.container.getRange({p: from}, {p: to}, true), (result) => result[1]);
  }

  async _remove(p) {
    const doc = await this._findOne(p);
    return this.container.delete(doc);
  }
}

class SortedArray {
  constructor(initial = [], by = (a) => a) {
    this.by = by;
    this.container = initial;
    if (this.container.length > 0) {
      this.container = _.sortBy(this.container, this.by);
    }
  }

  indexOf(input) {
    const index = _.sortedIndexBy(this.container, input, this.by);
    if (this.by(this.container[index]) == this.by(input)) {
      return index;
    }
    return -1;
  }

  get(input) {
    const index = this.indexOf(input);
    if (index >= 0) {
      return this.container[index];
    }
    return null;
  }

  getRange(from, to, includesHigh = false) {
    const indexFrom = _.sortedIndexBy(this.container, from, this.by);
    if (indexFrom >= 0) {
      if (includesHigh) {
        const indexTo = _.sortedLastIndexBy(this.container, to, this.by);
        if (indexTo >= 0) {
          return this.container.slice(indexFrom, indexTo);
        }
      } else {
        const indexTo = _.sortedIndexBy(this.container, to, this.by);
        if (indexTo >= 0) {
          return this.container.slice(indexFrom, indexTo);
        }
      }
    }
    return [];
  }

  set(inputs) {
    const _set = (input) => {
      const index = _.sortedIndexBy(this.container, input, this.by);
      this.container.splice(index, 0, input);
    };
    if (_.isArray(inputs)) {
      for (const input of inputs) {
        _set(input);
      }
    } else {
      _set(inputs);
    }
  }

  count() {
    return this.container.length;
  }

  del(input) {
    const index = _.sortedIndexBy(this.container, input, this.by);
    if (index >= 0) {
      return this.container[index];
    }
    return null;
  }
}

class TestSortedArray extends Test {
  constructor(seeds, points) {
    super(seeds, points);
  }

  async _prepare() {
    this.container = new SortedArray([], (a) => a.p);
  }

  async _bulkInsert(objs) {
    return this.container.set(objs);
  }

  async size() {
    return this.container.count();
  }

  async _insert(obj) {
    return this.container.set(obj);
  }

  async _findOne(p) {
    return this.container.get({p});
  }

  async _range(from, to) {
    return this.container.getRange({p: from}, {p: to}, true);
  }

  async _remove(p) {
    return this.container.del({p});
  }
}

async function test(times) {
  const seeds = generateSeeds(times);
  console.log({seeds: seeds.length});
  const results = [];
  results.push(await new TestArray(seeds, points).test());
  results.push(await new TestLoki(seeds, points).test());
  results.push(await new TestNedb(seeds, points).test());
  results.push(await new TestTingodb(seeds, points).test());
  results.push(await new TestTaffy(seeds, points).test());
  results.push(await new TestBTree(seeds, points).test());
  results.push(await new TestSortedArray(seeds, points).test());
  console.table(results);
  return [seeds.length, results];
}

(async () => {
  const results = [];
  for( const times of [1, 5, 10, 50, 100, 300, 500, 700, 1000]) {
    results.push(await test(times));
  }
  const header = ['class'].concat(_.map(results, (result) => result[0]));
  const names = _.map(results[0][1], (r) => r.name);
  const funcs = _.filter(_.keys(results[0][1][0]), (field) => field != 'name');

  const tableByFunc = _.chain(funcs).map((func) => [func, _.chain(names).map((name) => [name, []]).fromPairs().value()]).fromPairs().value();
  for (const result of results) {
    const records = result[0];
    const resultByName = _.keyBy(result[1], 'name');

    for (const func in tableByFunc) {
      for (const name in tableByFunc[func]) {
        tableByFunc[func][name].push(resultByName[name][func]);
      }
    }
  }
  for (const func in tableByFunc) {
    console.log(`==== ${func} ====`);
    console.log(header.join(','));
    for (const name in tableByFunc[func]) {
      console.log([name].concat(tableByFunc[func][name]).join(','));
    }
  }
})();

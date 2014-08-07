/**
 * 针对node-mysql的Active Record实现[Promise]
 * @version 1.0.0-Beta
 */

var q = require('q');

/**
 * 构造函数
 * @param {[type]} db 传入node-mysql的已获取到的连接 或 pool连接
 */
function ActiveRecord (db) {
	this._db = db;
	this._lastSql = '';
	this._emptyClause();
}

/**
 * 清空条件缓存，每次执行sql之前，会调用此函数
 * @return {[type]} [description]
 */
ActiveRecord.prototype._emptyClause = function () {
	this._clause = {
		'select': '',
		'table': '',
		'join': [],
		'set': [],
		'where': [],
		'orderby': {},
		'limit': null
	};
};

//完成本次sql，记录最后一次sql语句，并清空条件缓存
ActiveRecord.prototype._endSql = function (sql) {
	this._lastSql = sql;
	this._emptyClause();
};

//获取最近执行的一条sql
ActiveRecord.prototype.getLastSql = function () {
	return this._lastSql;
};

//--------------------------------------【SQL分析函数】

/**
 * 筛选字段
 * [仅用于SELECT]
 * @param  {[type]} fields 原生字符串，如'f1,f2' 或 '*'
 * @return {[type]}        [description]
 */
ActiveRecord.prototype.select = function (fields) {
	this._clause['select'] = fields;
	return this;
};

/**
 * 要操作的表
 * [可用于SELECT, UPDATE, DELETE]
 * @param  {[type]} table 原生字符串，如 'table1 t1, table2 t2'
 * @return {[type]}       [description]
 */
ActiveRecord.prototype.from = function (table) {
	this._clause['table'] = table;
	return this;
};

/**
 * 加入where语句
 * [可用于SELECT, UPDATE, DELETE]
 * [支持格式]
 * 示例1(键值对)：where({'f1':'v1','f2':'v2'}) （该形式相当于多次调用where()）
 * 示例2(原生语句)：where('AND `f1` = 123') （该形式需要手动过滤sql注入）
 * 示例3(混合数组)：where(['AND `f1`="xxx"', 'OR 1=1', {'OR abc >=': '131'}])
 * 示例4(常规)：where('OR `abc` <', 123)
 * [key的格式]
 * 'OR abc <=' （若“OR”不存在，默认为“AND”，若“<=”不存在，默认为“=”）
 * @param  {[type]} key   [description]
 * @param  {[type]} value [description]
 * @return {[type]}       [description]
 */
ActiveRecord.prototype.where = function (key, value) {
	if (typeof value === 'undefined') { //【仅1个参数的情况】
		if (typeof key === 'string') { //原生where语句
			this._clause['where'].push(key);
		} else if (Array.isArray(key)) { //多种形式的数组
			for (var i = 0; i < key.length; i++) this.where(key[i]);
		} else if (typeof key === 'object') { //键值对（相当于多次调用where）
			for (var k in key) this.where(k, key[k]);
		} else { //非法key
			throw new Error('参数1的格式有误');
		}
	} else if (typeof key !== 'undefined') { //【2个参数的情况】
		//key为字段，value为值
		var trans = this._transWhereKeyStr(key);
		trans['value'] = value;
		this._clause['where'].push(trans); //存储解析后的对象
	} else {
		throw new Error('参数有误');
	}
	return this;
};
//解析where中key的不同方式（【注】没有进行sql检测）
ActiveRecord.prototype._transWhereKeyStr = function (keystr) {
	var relate = 'AND', field = null, operate = '=';
	var parts = keystr.split(' ');
	if (parts.length === 1) {
		field = parts[0];
	} else if (parts.length === 2) {
		var part1 = parts[0].toUpperCase().trim();
		if (['AND', 'OR'].indexOf(part1) !== -1) { //"AND field1"的形式
			relate = part1, field = parts[1];
		} else { //"field1 >="的形式
			field = part1, operate = parts[1];
		}
	} else if (parts.length >= 3) {
		relate = parts[0], field = parts[1], operate = parts[2];
	}
	return {
		'relate': relate,
		'field': field,
		'operate': operate,
	};
};

/**
 * 加入join
 * [可用于SELECT, UPDATE]
 * [示例]
 * 1.join('RIGHT', 'table1', '`f1`=`f2`')
 * 2.join('`table1` t1', 't1.f1=t2.f2')
 * 3.join('LEFT JOIN table2 t2 ON(t2.p1 = t3.p1)')
 * @param  {[type]} relate 可忽略该参数
 * @param  {[type]} table  [description]
 * @param  {[type]} on     字符串，填写ON()里面的条件语句
 * @return {[type]}        [description]
 */
ActiveRecord.prototype.join = function (relate, table, on) {
	if (arguments.length === 1) { //原生形式
		this._clause['join'].push(relate);
	} else {
		if (arguments.length === 2) { //无relate的形式
			on = table;
			table = relate;
			relate = 'LEFT';
		}
		this._clause['join'].push({
			'relate': relate,
			'table': table,
			'on': on
		});
	}
	return this;
};

/**
 * 用于UPDAET,INSERT设置值
 * [可用于UPDATE, INSERT]
 * @param {[type]} key   字段 或 原生语句 或 混合数组 或 键值对
 * @param {[type]} value 值 或 忽略此参数表示key为键值对 或 原生语句
 */
ActiveRecord.prototype.set = function (key, value) {
	if (typeof value === 'undefined') {
		if (typeof key === 'string') {
			this._clause['set'].push(key);
		} else if (Array.isArray(key)) { //原生语句如：f1=f1+1
			for (var i = 0; i < key.length; i++) this.set(key[i]);
		} else if (typeof key === 'object') { //键值对
			for (var k in key) this.set(k, key[k]);
		}
	} else {
		this._clause['set'].push({
			'field': key,
			'value': value
		});
	}
	return this;
};

/**
 * 加入orderby
 * [可用于 SELECT, UPDATE, DELETE]
 * @param  {[type]} field 支持原生语句
 * @param  {[type]} order 顺序，可为空
 * @return {[type]}       [description]
 */
ActiveRecord.prototype.orderby = function (field, order) {
	if (typeof order !== 'undefined') field += ' ' + order; //转换为原生
	var parts = field.split(',');
	for (var i = 0; i < parts.length; i++) {
		var tmp = parts[i].split(' ');
		this._clause['orderby'][tmp[0]] = tmp[1];
	}
	return this;
};


/**
 * 更改limit
 * [可用于 SELECT, UPDATE, DELETE]
 * [示例]
 * 1.limit(10, 20)
 * 2.limit(1)
 * @param  {[type]} offset [description]
 * @param  {[type]} num    [description]
 * @return {[type]}        [description]
 */
ActiveRecord.prototype.limit = function (offset, num) {
	if (arguments.length === 1) {
		num = offset;
		offset = 0;
	}
	this._clause['limit'] = {
		'offset': parseInt(offset) || 0,
		'num': parseInt(num) || 0
	};
	return this;
};
/**
 * 分页方式更改limit
 * @param  {[type]} page    范围：>=1，默认1
 * @param  {[type]} perpage 范围：>=1，默认20
 * @return {[type]}         [description]
 */
ActiveRecord.prototype.limitPage = function (page, perpage) {
	page = parseInt(page) || 0; if (page < 1) page = 1;
	perpage = parseInt(perpage) || 0; if (perpage < 1) perpage = 20;
	return this.limit((page - 1) * perpage, perpage);
};

//--------------------------------------【SQL构建函数】

ActiveRecord.prototype._strSelect = function () {
	return this._clause['select'] || '*';
};

ActiveRecord.prototype._strTable = function () {
	return this._clause['table'];
};

/**
 * 将join转换为sql字符串
 * @return {[type]} [description]
 */
ActiveRecord.prototype._strJoin = function () {
	var ret = [];
	for (var i = 0; i < this._clause['join'].length; i ++) {
		var tmp = this._clause['join'][i];
		if (typeof tmp === 'string' && tmp) {
			ret.push(tmp);
		} else {
			ret.push(tmp['relate'] + ' ' + tmp['table'] + ' ' + 'ON(' + tmp['on'] + ')');
		}
	}
	return ret.join(' ');
};

ActiveRecord.prototype._strWhere = function () {
	var ret = [];
	for (var i = 0; i < this._clause['where'].length; i ++) {
		var tmp = this._clause['where'][i];
		if (typeof tmp === 'string' && tmp) {
			ret.push(tmp);
		} else {
			ret.push(tmp['relate'] + ' ' + tmp['field'] + ' ' + tmp['operate'] + ' ' + this._db.escape(tmp['value']));
		}
	}
	return ret.join(' ');
};

ActiveRecord.prototype._strSet = function () {
	var ret = [];
	for (var i = 0; i < this._clause['set'].length; i++) {
		var tmp = this._clause['set'][i];
		if (typeof tmp === 'string' && tmp) {
			ret.push(tmp);
		} else {
			ret.push(tmp['field'] + '=' + this._db.escape(tmp['value']));
		}
	}
	return ret.join(', ');
};

ActiveRecord.prototype._strOrderBy = function () {
	var ret = [];
	for (var field in this._clause['orderby']) {
		ret.push(field + ' ' + this._clause['orderby'][field]);
	}
	return ret.join(', ');
};

ActiveRecord.prototype._strLimit = function () {
	return this._clause['limit'] ? this._clause['offset'] + ', ' + this._clause['num'] : '';
};

//--------------------------------------【SQL执行函数】

ActiveRecord.prototype.get = function (table) {
	if (table) this.from(table);

	var where = this._strWhere();
	var orderby = this._strOrderBy();
	var limit = this._strLimit();
	//---组装SQL
	var sql = [
		'SELECT',
			this._strSelect(),
		'FROM',
			this._strTable(),
			this._strJoin(),
		where ? 'WHERE ' + where : '',
		orderby ? 'ORDER BY ' + orderby : '',
		limit ? 'LIMIT ' + limit : ''
	].join(' ');
	//---结束本轮
	this._endSql(sql);
	//---执行sql
	return q.ninvoke(this._db, 'query', sql).spread(function (rows) { return rows || []; });
};

ActiveRecord.prototype.getOne = function (table) {
	this.limit(1);
	return this.get(table).then(function (rows) { return rows[0] || null; });
};

/**
 * 插入一条数据，返回新记录ID
 * @param  {[type]} table [必须]表名
 * @param  {[type]} data  [可选]键值对
 * @return {[type]}       [description]
 */
ActiveRecord.prototype.insert = function (table, data) {
	if (!table) throw new Error('参数1必须');
	if (data) this.set(data);
	var sql = 'INSERT INTO ' + table + ' SET ' + this._strSet();
	//---结束本轮
	this._endSql(sql);
	//执行
	return q.ninvoke(this._db, 'query', sql).spread(function (result) { return result.insertId; });
};

/**
 * 更新数据，返回改变记录数
 * @param  {[type]} table [可选]表名
 * @param  {[type]} data  [可选]键值对
 * @param {[type]} where [可选]键值对
 * @return {[type]}       [description]
 */
ActiveRecord.prototype.update = function (table, data, where) {
	if (table) this.from(table);
	if (data) this.set(data);
	if (where) this.where(where);

	var where = this._strWhere();
	var orderby = this._strOrderBy();
	var limit = this._strLimit();
	var sql = [
		'UPDATE',
			this._strTable(),
			this._strJoin(),
		'SET',
			this._strSet(),
		where ? 'WHERE ' + where : '',
		orderby ? 'ORDER BY ' + orderby : '',
		limit ? 'LIMIT ' + limit : ''
	].join(' ');
	//---结束本轮
	this._endSql(sql);
	//执行
	return q.ninvoke(this._db, 'query', sql).spread(function (result) { return result.changedRows; });
};

/**
 * 删除记录
 * @param  {[type]} table [可选]表名
 * @param  {[type]} where [可选]键值对
 * @return {[type]}       [description]
 */
ActiveRecord.prototype.delete = function (table, where) {
	if (table) this.from(table);
	if (where) this.where(where);

	var where = this._strWhere();
	var orderby = this._strOrderBy();
	var limit = this._strLimit();
	var sql = [
		'DELETE FROM',
			this._strTable(),
		where ? 'WHERE ' + where : '',
		orderby ? 'ORDER BY ' + orderby : '',
		limit ? 'LIMIT ' + limit : ''
	].join(' ');
	//---结束本轮
	this._endSql(sql);
	//执行
	return q.ninvoke(this._db, 'query', sql).spread(function (result) { return result.affectedRows; });
};

module.exports = ActiveRecord;
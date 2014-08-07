var MysqlAR = require('mysql_ar');

var assert = require('assert');
describe('测试1', function () {
	describe('#indexOf()', function () {
		it('若不存在，返回-1', function(){
			assert.equal(-1, [1,2,3].indexOf(5));
			assert.equal(-1, [1,2,3].indexOf(0));
		})
	});
});
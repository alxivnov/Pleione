(function (e, t) {
	typeof exports == 'object' && typeof module != 'undefined'
		? module.exports = t()
		: typeof define == 'function' && define.amd
			? define(t)
			: e.QueryChain = t()
})(this, function () {
	"use strict";

	// Builds chained SQL commands into request.
	const QueryBuild = require('./query_build.js');

	return class QueryFetch extends QueryBuild {

	}
})

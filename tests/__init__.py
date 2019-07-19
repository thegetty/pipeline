import pprint

def merge_lists(l, r):
	'''
	Given two lists `l` and `r`, return a generator of the combined items from both lists.

	If any two items l' in l and r' in r are both `dict`s and have the same value for the
	`id` key, they will be `merge`d and the resulting `dict` included in the results in
	place of l' or r'.
	'''
	identified = {}
	all_items = l + r
	other = []
	for item in all_items:
		try:
			item_id = item['id']
			if item_id in identified:
				identified[item_id] += [item]
			else:
				identified[item_id] = [item]
		except:
			other.append(item)

	for ident, items in identified.items():
		r = items[:]
		while len(r) > 1:
			a = r.pop(0)
			b = r.pop(0)
			m = merge(a, b)
			r.insert(0, m)
		yield from r

	yield from other

def merge(l, r):
	'''
	Given two items `l` and `r` of the same type, merge their contents and return the
	result. Raise an exception if `l` and `r` are of differing types.

	If the items are of type `dict`, recursively merge any values with shared keys, and
	also include data from any non-shared keys. If `l` and `r` both have values for the
	`id` key and they differ in value, raise an exception.

	If the items are of type `list`, merge them with `merge_lists`.

	If the items are of type `str` or `bytes`, return the value if `l` and `r` are equal.
	Otherwise raise and exception.
	'''
	if l is None:
		return r
	if r is None:
		return l

	if not isinstance(l, type(r)):
		pprint.pprint(l)
		pprint.pprint(r)
		raise Exception('bad data in json merge')

	if isinstance(l, dict):
		keys = set(list(l.keys()) + list(r.keys()))
		intersection = {k for k in keys if k in l and k in r}
		if 'id' in intersection:
			lid = l['id']
			rid = r['id']
			if lid != rid:
				pprint.pprint(l)
				pprint.pprint(r)
				raise Exception('attempt to merge two dicts with different ids: (%r, %r)' % (lid, rid))
		return {k: merge(l.get(k), r.get(k)) for k in keys}
	elif isinstance(l, list):
		return list(merge_lists(l, r))
	elif isinstance(l, (str, bytes)):
		if l == r:
			return l
		else:
			raise Exception('data conflict: %r <=> %r' % (l, r))
	else:
		raise NotImplementedError('unhandled type: %r' % (type(l)))
	return l

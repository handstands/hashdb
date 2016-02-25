#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os.path
import sqlite3

sqldb = 'hashdb.sql'

c = 0
if os.path.exists(sqldb):
	conn = sqlite3.connect(sqldb)
	for entry, in conn.cursor().execute('SELECT path FROM entries'):
		if not os.path.exists(entry):
			print "%s not found. Removing." % entry
			conn.cursor().execute('DELETE FROM entries WHERE path = ?', (entry, ))
			conn.commit()
			c += 1

print "%d entries removed." % c
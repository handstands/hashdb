#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os.path
import hashlib
import time
import sqlite3
import argparse

base = '/data/misc/'
extensions = ['.flv', '.mov', '.mp4', '.wmv', '.avi', '.mkv']

def grabfiles(dirname):
	files = []
	for entry in os.listdir(dirname):
		n = os.path.join(dirname, entry)
		if os.path.isdir(n) and entry != "lost+found":
			files += grabfiles(n)
		elif os.path.splitext(entry)[1] in extensions:
			files.append(n)
	return files		

def gethash(filename):
	h  = hashlib.sha1()
	f = open(filename)
	d = f.read()
	f.close()
	h.update(d)
	return h.hexdigest()


def updatedb(basedir, db):
	t_start = time.time()
	bytes = 0
	db.cursor().execute('UPDATE entries SET chk = 0')
	for f in grabfiles(basedir):
		a = os.path.abspath(f)
		try:
			c = db.cursor().execute('SELECT mtime FROM entries WHERE path = ?', (a, )).fetchone()
		except sqlite3.ProgrammingError:
			print "Error: %s" % a
			continue
		if not c:
			print "[%s]: %s" % (time.ctime(), a)
			bytes += os.stat(f).st_size
			db.cursor().execute('INSERT INTO entries (hex, mtime, path, chk) VALUES(?,?,?,?)', (gethash(f), os.path.getmtime(f), a, 1))
			db.commit()
		elif os.path.getmtime(f) > c[0]:
			print "[%s]: %s" % (time.ctime(), a)
			bytes += os.stat(f).st_size
			db.cursor().execute('UPDATE entries SET hex = ?, mtime = ?, chk = ? WHERE path = ?', (gethash(f), os.path.getmtime(f), 1, a))
			db.commit()
		else:
			pass
			#db.cursor().execute('UPDATE entries SET chk = 1 WHERE path = ?', (a,))
			#db.commit()
	return bytes, time.time()-t_start

hashfile = os.path.expanduser("~/.hashdb.db")

parser = argparse.ArgumentParser(description='This is a script designed to create a persistent database of file hashes to aid in the detection of undesirable duplicates.')
parser.add_argument('-d','--directory', help='Base directory from which all the children are to be scanned.',required=True)

args = vars(parser.parse_args())

if not os.path.exists(hashfile):
	conn = sqlite3.connect(hashfile)
	conn.cursor().execute('CREATE TABLE entries (hex, mtime INTEGER, path, chk INTEGER)')
	conn.commit()

conn = sqlite3.connect(hashfile)
s, t = updatedb(args['directory'], conn)

if s:
	print "Hashed %d bytes in %d seconds. %d bytes/second." % (s, t, s/t)
c, tot = 0, 0
u_hex = conn.cursor().execute('SELECT DISTINCT(hex) FROM entries').fetchall()
for hx, in u_hex:
	r = conn.cursor().execute('SELECT path FROM entries WHERE hex = ?', (hx, )).fetchall()
	if len(r) > 1:
		r = [e[0] for e in r]
		c += len(r)-1
		for p in r:
			if os.path.exists(p):
				tot += (len(r)-1)*os.stat(p).st_size
				break
		print "Matching files: \"%s\"" % '", "'.join(r)
print "%d duplicate files for %d bytes." % (c, tot)
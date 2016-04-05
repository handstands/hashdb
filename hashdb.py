#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
import os.path
import hashlib
import time
import sqlite3
import argparse

default_extensions = ['.flv', '.mov', '.mp4', '.wmv', '.avi', '.mkv']

def grabfiles(dirname, extensions):
	valid_files = []
	for root, dirs, files in os.walk(dirname):
		valid_files += [os.path.join(root, file_) for file_ in files if os.path.splitext(file_)[1] in extensions]
		for dir_ in dirs:
			valid_files += grabfiles(dir_, extensions)
	return valid_files

def filechunk(file_object, chunk):
	while True:
		data = file_object.read(chunk)
		if not data:
			break
		yield data

def gethash(filename):
	h  = hashlib.sha1()
	with open(filename, 'rb') as f:
		for block in filechunk(f, 4096):
			h.update(block)
	return h.hexdigest()

def prunedeadwood(db, quiet):
	c = 0
	for entry, in db.cursor().execute('SELECT path FROM entries'):
		if not os.path.exists(entry):
			if not quiet:
				print("%s not found. Removing." % entry)
			db.cursor().execute('DELETE FROM entries WHERE path = ?', (entry, ))
			conn.commit()
			c += 1
	print("%d entries removed." % c)

def matchfiles(db):
	c, tot = 0, 0
	u_hex = db.cursor().execute('SELECT DISTINCT(hex) FROM entries').fetchall()
	for hx, in u_hex:
		r = db.cursor().execute('SELECT path FROM entries WHERE hex = ?', (hx, )).fetchall()
		if len(r) > 1:
			r = [e[0] for e in r]
			c += len(r)-1
			for p in r:
				if os.path.exists(p):
					tot += (len(r)-1)*os.stat(p).st_size
					break
			print("Matching files: \"%s\"" % '", "'.join(r))
	print("%d duplicate files for %d bytes." % (c, tot))

def updatedb(basedir, db, quiet, extensions):
	t_start = time.time()
	bytes = 0
	for f in grabfiles(basedir, extensions):
		a = os.path.abspath(f)
		try:
			c = db.cursor().execute('SELECT mtime FROM entries WHERE path = ?', (a, )).fetchone()
		except sqlite3.ProgrammingError:
			print("Error: %s" % a)
			continue
		if not c:
			if not quiet:
				print("[%s]: %s" % (time.ctime(), a))
			bytes += os.stat(f).st_size
			db.cursor().execute('INSERT INTO entries (hex, mtime, path) VALUES(?,?,?)', (gethash(f), os.path.getmtime(f), a))
			db.commit()
		elif os.path.getmtime(f) > c[0]:
			if not quiet:
				print("[%s]: %s" % (time.ctime(), a))
			bytes += os.stat(f).st_size
			db.cursor().execute('UPDATE entries SET hex = ?, mtime = ? WHERE path = ?', (gethash(f), os.path.getmtime(f), a))
			db.commit()
	return bytes, time.time()-t_start

hashfile = os.path.expanduser("~/.hashdb.db")

parser = argparse.ArgumentParser(description='This is a script designed to create a persistent database of file hashes to aid in the detection of undesirable duplicates.')
parser.add_argument('-d', '--directory', help='Base directory from which all the children are to be scanned.', required=True)
parser.add_argument('-q', '--quiet', help='Quiet mode. Script will not output anything related on ongoing hashing operations.', required=False, default=False, action='store_true')
parser.add_argument('-c', '--clean-up', help='Cleanup-mode. This remove every file from the database that cannot be found at it\'s location.', action='store_true', required=False, default=False)
parser.add_argument('--skip-match', help='Hash only-mode. This makes it that only the file hashing process takes place and skips the matching.', action='store_true', required=False, default=False)
parser.add_argument('--skip-hash', help='Match only-mode. This makes it that only the file matching process takes place and skips the hashing.', action='store_true', required=False, default=False)
parser.add_argument('--extensions', help='A list of extensions the script should use in determining which files are to be hashed. Default extensions are %s. Setting this will override them.' % ', '.join(default_extensions), action='append', required=False, default=[], nargs='+')
args = vars(parser.parse_args())

if not os.path.exists(hashfile):
	conn = sqlite3.connect(hashfile)
	conn.cursor().execute('CREATE TABLE entries (hex, mtime INTEGER, path)')
	conn.commit()

conn = sqlite3.connect(hashfile)

if args['extensions']:
	extensions = args['extensions']
else:
	extensions = default_extensions

if args['clean_up']:
	prunedeadwood(conn, args['quiet']) 

if not args['skip_hash']:
	s, t = updatedb(args['directory'], conn, args['quiet'], extensions)
	if s:
		print("Hashed %d bytes in %d seconds. %d bytes/second." % (s, t, s/t))

if not args['skip_match']:
	matchfiles(conn)

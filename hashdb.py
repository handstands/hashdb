#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
"""hashdb.py provides an easy way to create and maintain a persistent record of file hashes to aid in their semi-automatic deduplication"""
import os.path
import hashlib
import time
import sqlite3
import argparse

DEFAULT_EXTENSIONS = ['.flv', '.mov', '.mp4', '.wmv', '.avi', '.mkv']

def grabfiles(dirname, extensions_):
	"""grabfiles returns all the files matching the supplied extensions while recursing through the directory"""
	valid_files = []
	for root, dirs, files in os.walk(dirname):
		valid_files += [os.path.join(root, file_) for file_ in files if os.path.splitext(file_)[1] in extensions_]
		for dir_ in dirs:
			valid_files += grabfiles(dir_, extensions_)
	return valid_files

def _filechunk(file_object, chunk):
	"""helper for the gethash function"""
	while True:
		data = file_object.read(chunk)
		if not data:
			break
		yield data

def gethash(filename):
	"""gethash returns a file's SHA-1 hash"""
	hasher = hashlib.sha1()
	with open(filename, 'rb') as file_:
		for block in _filechunk(file_, 4096):
			hasher.update(block)
	return hasher.hexdigest()

def prunedeadwood(database, quiet):
	"""prunedeadwood removes all references from the database that are no longer found on the filesystem"""
	tally = 0
	for entry, in database.cursor().execute('SELECT path FROM entries'):
		if not os.path.exists(entry):
			if not quiet:
				print("%s not found. Removing." % entry)
			database.cursor().execute('DELETE FROM entries WHERE path = ?', (entry, ))
			database.commit()
			tally += 1
	print("%d entries removed." % tally)

def matchfiles(database):
	"""matchfiles prints all the files that have duplicates"""
	tally, total = 0, 0
	u_hex = database.cursor().execute('SELECT DISTINCT(hex) FROM entries').fetchall()
	for hex_hash, in u_hex:
		rows = database.cursor().execute('SELECT path FROM entries WHERE hex = ?', (hex_hash, )).fetchall()
		if len(rows) > 1:
			rows = [e[0] for e in rows]
			tally += len(rows)-1
			for path_ in rows:
				if os.path.exists(path_):
					total += (len(rows)-1)*os.stat(path_).st_size
					break
			print("Matching files: \"%s\"" % '", "'.join(rows))
	print("%d duplicate files for %d bytes." % (tally, total))

def updatedb(basedir, database, quiet, extensions_):
	"""updatedb trawls recursively through basedir and adds new files and updates changed files"""
	t_start = time.time()
	bytes_ = 0
	for file_ in grabfiles(basedir, extensions_):
		abs_path = os.path.abspath(file_)
		try:
			current = database.cursor().execute('SELECT mtime FROM entries WHERE path = ?', (abs_path, )).fetchone()
		except sqlite3.ProgrammingError:
			print("Error: %s" % abs_path)
			continue
		if not current:
			if not quiet:
				print("[%s]: %s" % (time.ctime(), abs_path))
			bytes_ += os.stat(file_).st_size
			database.cursor().execute('INSERT INTO entries (hex, mtime, path) VALUES(?,?,?)', (gethash(file_), os.path.getmtime(file_), abs_path))
			database.commit()
		elif os.path.getmtime(file_) > current[0]:
			if not quiet:
				print("[%s]: %s" % (time.ctime(), abs_path))
			bytes_ += os.stat(file_).st_size
			database.cursor().execute('UPDATE entries SET hex = ?, mtime = ? WHERE path = ?', (gethash(file_), os.path.getmtime(file_), abs_path))
			database.commit()
	return bytes_, time.time()-t_start

HASHFILE = os.path.expanduser("~/.hashdb.db")

PARSER = argparse.ArgumentParser(description='This is a script designed to create a persistent database of file hashes to aid in the detection of undesirable duplicates.')
PARSER.add_argument('-d', '--directory', help='Base directory from which all the children are to be scanned.', required=True)
PARSER.add_argument('-q', '--quiet', help='Quiet mode. Script will not output anything related on ongoing hashing operations.', required=False, default=False, action='store_true')
PARSER.add_argument('-c', '--clean-up', help='Cleanup-mode. This remove every file from the database that cannot be found at it\'s location.', action='store_true', required=False, default=False)
PARSER.add_argument('--skip-match', help='Hash only-mode. This makes it that only the file hashing process takes place and skips the matching.', action='store_true', required=False, default=False)
PARSER.add_argument('--skip-hash', help='Match only-mode. This makes it that only the file matching process takes place and skips the hashing.', action='store_true', required=False, default=False)
PARSER.add_argument('--extensions', help='A list of extensions the script should use in determining which files are to be hashed. Default extensions are %s. Setting this will override them.' % ', '.join(DEFAULT_EXTENSIONS), action='append', required=False, default=[], nargs='+')
ARGS = vars(PARSER.parse_args())

if not os.path.exists(HASHFILE):
	CONN = sqlite3.connect(HASHFILE)
	CONN.cursor().execute('CREATE TABLE entries (hex, mtime INTEGER, path)')
	CONN.commit()

CONN = sqlite3.connect(HASHFILE)

if ARGS['extensions']:
	EXTENSIONS = ARGS['extensions']
else:
	EXTENSIONS = DEFAULT_EXTENSIONS

if ARGS['clean_up']:
	prunedeadwood(CONN, ARGS['quiet'])

if not ARGS['skip_hash']:
	BYTES_, TALLY = updatedb(ARGS['directory'], CONN, ARGS['quiet'], EXTENSIONS)
	if BYTES_ != 0:
		print("Hashed %d bytes in %d seconds. %d bytes/second." % (BYTES_, TALLY, BYTES_/TALLY))

if not ARGS['skip_match']:
	matchfiles(CONN)

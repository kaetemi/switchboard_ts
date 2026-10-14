/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

/// The peer protocol's plumbing: JSON lines on stdout, errors as JSON,
/// handles, and the argument readers every command uses. The protocol is
/// specified in test/interop/README.md.
library;

import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

import 'package:switchboard/core.dart';

/// Version of the peer protocol, reported in `READY`.
const int peerProtocolVersion = 1;

/// Bytes as upper-case hex separated by spaces, as the wiki writes them.
String hex(List<int> bytes) => hexString(bytes);

/// Bytes from hex, spaces optional; null and empty are no bytes.
Uint8List bytesOf(Object? value) {
  if (value == null) {
    return Uint8List(0);
  }
  if (value is! String) {
    throw ArgumentError('hex string expected, got ${value.runtimeType}');
  }
  return hexBytes(value);
}

/// One JSON line per call on stdout. Lines are written whole, so responses
/// and events never interleave.
class Output {
  /// An output on [sink], stdout by default.
  Output([IOSink? sink]) : _sink = sink ?? stdout;

  final IOSink _sink;

  /// Writes a raw line (only `READY` uses it).
  void line(String text) => _sink.writeln(text);

  /// The successful response to command [id].
  void respond(Object? id, Object? value) =>
      _write({'id': id, 'ok': true, 'value': value});

  /// The failed response to command [id].
  void fail(Object? id, Object error) =>
      _write({'id': id, 'ok': false, 'error': errorJson(error)});

  /// An unsolicited event.
  void event(String name, Map<String, Object?> fields) =>
      _write({'event': name, ...fields});

  void _write(Map<String, Object?> message) =>
      _sink.writeln(jsonEncode(message));

  /// Flushes stdout before the process exits.
  Future<void> flush() => _sink.flush();
}

/// [error] as the protocol's error object: `kind` is `status` for a
/// Switchboard status (with its `code` and `reason`), `format` for a
/// malformed input, `argument` for a bad command, `other` otherwise.
Map<String, Object?> errorJson(Object error) {
  final type = error.runtimeType.toString();
  if (error is SwitchboardException) {
    return {
      'kind': 'status',
      'type': type,
      'code': error.status.code,
      'reason': error.status.reason,
      'message': error.toString(),
    };
  }
  if (error is FormatException) {
    return {'kind': 'format', 'type': type, 'message': error.message};
  }
  if (error is ArgumentError || error is RangeError || error is StateError) {
    return {'kind': 'argument', 'type': type, 'message': error.toString()};
  }
  return {'kind': 'other', 'type': type, 'message': error.toString()};
}

/// A status as JSON: code and reason.
Map<String, Object?> statusJson(Status status) => {
  'code': status.code,
  'reason': status.reason,
};

/// The status a command names: `code` (default 0) and `reason`.
Status statusArg(Map<String, Object?> cmd, {int code = 0}) =>
    Status(intArg(cmd, 'code') ?? code, stringArg(cmd, 'reason') ?? '');

/// An optional integer argument.
int? intArg(Map<String, Object?> cmd, String key) {
  final v = cmd[key];
  if (v == null) {
    return null;
  }
  if (v is int) {
    return v;
  }
  if (v is double && v == v.truncateToDouble()) {
    return v.toInt();
  }
  throw ArgumentError('$key: integer expected, got $v');
}

/// An optional string argument.
String? stringArg(Map<String, Object?> cmd, String key) {
  final v = cmd[key];
  if (v == null || v is String) {
    return v as String?;
  }
  throw ArgumentError('$key: string expected, got $v');
}

/// A required string argument.
String requiredString(Map<String, Object?> cmd, String key) =>
    stringArg(cmd, key) ?? (throw ArgumentError('$key is required'));

/// An optional boolean argument.
bool boolArg(Map<String, Object?> cmd, String key, {bool fallback = false}) {
  final v = cmd[key];
  if (v == null) {
    return fallback;
  }
  if (v is bool) {
    return v;
  }
  throw ArgumentError('$key: boolean expected, got $v');
}

/// An optional duration argument in milliseconds.
Duration? msArg(Map<String, Object?> cmd, String key) {
  final ms = intArg(cmd, key);
  return ms == null ? null : Duration(milliseconds: ms);
}

/// An optional object argument.
Map<String, Object?>? objectArg(Map<String, Object?> cmd, String key) {
  final v = cmd[key];
  if (v == null) {
    return null;
  }
  if (v is Map<String, Object?>) {
    return v;
  }
  throw ArgumentError('$key: object expected, got $v');
}

/// The objects commands and services create, by string handle (`c1`,
/// `ch4`, `t2`, `s3`). Numbers are never reused within a process, so a
/// handle in a log names one object.
class Handles {
  final Map<String, Object> _objects = {};
  final Map<Object, String> _names = Map.identity();
  final Map<String, int> _counters = {};

  /// Registers [object] under a new handle starting with [prefix].
  String add(String prefix, Object object) {
    final existing = _names[object];
    if (existing != null) {
      return existing;
    }
    final n = (_counters[prefix] ?? 0) + 1;
    _counters[prefix] = n;
    final handle = '$prefix$n';
    _objects[handle] = object;
    _names[object] = handle;
    return handle;
  }

  /// The handle of [object], if it has one.
  String? nameOf(Object object) => _names[object];

  /// The object of [handle], which must be a [T].
  T get<T extends Object>(Object? handle, String what) {
    final object = handle is String ? _objects[handle] : null;
    if (object is! T) {
      throw ArgumentError('no $what with handle $handle');
    }
    return object;
  }

  /// Forgets [handle]'s object.
  void remove(Object object) {
    final handle = _names.remove(object);
    if (handle != null) {
      _objects.remove(handle);
    }
  }

  /// Every registered object, newest first.
  List<Object> get all => _objects.values.toList().reversed.toList();

  /// Forgets every object; the counters keep counting.
  void clear() {
    _objects.clear();
    _names.clear();
  }

  /// The number of registered objects.
  int get length => _objects.length;
}

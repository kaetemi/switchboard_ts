/*
Polyverse Switchboard
Microservice Network Architecture
Copyright (C) 2018-2026  Jan BOON (Kaetemi)
*/

// The Dart Switchboard as a test peer for the TypeScript interop tests.
// See test/interop/README.md for the protocol.

import 'package:switchboard_dart_peer/src/peer.dart';

Future<void> main(List<String> args) => runPeer(args);

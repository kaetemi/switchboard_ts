// Produces endpoint-oracle.json: reads a JSON array of candidate endpoint
// strings (args[0]) and writes, for each, the text form the Dart reference
// decoder accepts it as, or null when it refuses it (args[1]). The decoder
// is `ServiceRecord`'s: `Uri.parse`, then the scheme, length and stability
// check of `ServiceRecord.checkEndpoint` (switchboard lib/src/naming/
// naming_protocol.dart). Run with `dart run endpoint-oracle.dart in out`.
import 'dart:convert';
import 'dart:io';

String? problem(Uri uri) {
  if (!uri.hasScheme) return 'no scheme';
  final text = uri.toString();
  if (utf8.encode(text).length > 0xFF) return 'long';
  final again = Uri.tryParse(text);
  if (again == null || again != uri || uri != again) return 'unstable';
  return null;
}

void main(List<String> args) {
  final inputs =
      (jsonDecode(File(args[0]).readAsStringSync()) as List).cast<String>();
  final out = <List<Object?>>[];
  for (final s in inputs) {
    if (utf8.encode(s).length > 255) {
      out.add([s, null]);
      continue;
    }
    Uri uri;
    try {
      uri = Uri.parse(s);
    } catch (_) {
      // FormatException, and a RangeError for some IPv6 literals.
      out.add([s, null]);
      continue;
    }
    out.add([s, problem(uri) == null ? uri.toString() : null]);
  }
  File(args[1]).writeAsStringSync(jsonEncode(out));
}

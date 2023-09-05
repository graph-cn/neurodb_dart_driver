// Copyright (c) 2023- All neurodb_dart_driver authors. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

import 'package:neurodb_dart_driver/neurodb_dart_driver.dart';
import 'package:test/test.dart';

void main() {
  group('A group of tests', () {
    var driver = NeuroDBDriver("127.0.0.1", 8839);

    setUp(() {
      // do nothing.
    });

    test('First Test', () {
      var result = driver.executeQuery("match (n)-[r]->(m) return n,r,m ");
      print("$result");
      print('ok');
    });

    test('test msg type handle', () async {
      var result = await driver.executeQuery("show databases");
      print("$result");
      print('ok');
    });
  });
}

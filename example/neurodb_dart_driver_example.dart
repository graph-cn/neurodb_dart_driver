// Copyright (c) 2023- All neurodb_dart_driver authors. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

import 'package:neurodb_dart_driver/neurodb_dart_driver.dart';

void main() async {
  var driver = NeuroDBDriver("127.0.0.1", 8839);
  ResultSet resultSet = await driver.executeQuery("match (n) return n");
  print("$resultSet");
  resultSet = await driver.executeQuery("match (n)-[r]->(m) return n,r,m ");
  print("$resultSet");
  print("ok");
  driver.close();
}

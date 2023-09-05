// Copyright (c) 2023- All neurodb_dart_driver authors. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

// ignore_for_file: constant_identifier_names, curly_braces_in_flow_control_structures, avoid_init_to_null

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';

const int NEURODB_RETURNDATA = 1;
const int NEURODB_SELECTDB = 2;
const int NEURODB_EOF = 3;
const int NEURODB_NODES = 6;
const int NEURODB_LINKS = 7;
const int NEURODB_EXIST = 17;
const int NEURODB_NIL = 18;
const int NEURODB_RECORD = 19;
const int NEURODB_RECORDS = 20;

const int NDB_6BITLEN = 0;
const int NDB_14BITLEN = 1;
const int NDB_32BITLEN = 2;
const int NDB_ENCVAL = 3;
// const int NDB_LENERR =UINT_MAX;

const int VO_STRING = 1;
const int VO_NUM = 2;
const int VO_STRING_ARRY = 3;
const int VO_NUM_ARRY = 4;
const int VO_NODE = 5;
const int VO_LINK = 6;
const int VO_PATH = 7;
const int VO_VAR = 8;
const int VO_VAR_PATTERN = 9;

/// The node entity of neurodb.
/// neurodb 的节点实体。
class Node {
  int id;
  List<String> labels;
  Map<String, dynamic> properties;
  Node(this.id, this.labels, this.properties);
}

/// The link entity of neurodb.
/// neurodb 的关系实体。
class Link {
  int id;
  int startNodeId;
  int endNodeId;
  String? type;
  Map<String, dynamic> properties;
  Link(this.id, this.startNodeId, this.endNodeId, this.type, this.properties);
}

/// The value of neurodb.
/// neurodb 的字段值。
class ColVal {
  int type = 0;
  dynamic val;
  int aryLen = 0;

  /// Get num value if the type is [VO_NUM].
  /// 获取数值类型的值。
  double getNum() {
    return double.parse(val);
  }

  /// Get num array value if the type is [VO_NUM_ARRY].
  /// 获取数值数组类型的值。
  List<double> getNumArray() {
    return val;
  }

  /// Get string value if the type is [VO_STRING].
  /// 获取字符串类型的值。
  String getString() {
    return val;
  }

  /// Get string array value if the type is [VO_STRING_ARRY].
  /// 获取字符串数组类型的值。
  List<String> getStringArry() {
    return val;
  }

  /// Get node value if the type is [VO_NODE].
  /// 获取节点类型的值。
  Node getNode() {
    return val;
  }

  /// Get link value if the type is [VO_LINK].
  /// 获取关系类型的值。
  Link getLink() {
    return val;
  }

  /// Get path value if the type is [VO_PATH].
  /// 获取路径类型的值。
  List<dynamic> getPath() {
    return val;
  }
}

/// The result set of neurodb.
/// neurodb 的结果集。
class RecordSet {
  /// The labels of the result set.
  /// 结果集的标签。
  List<String> labels = [];

  /// The type names of the result set.
  /// 结果集的类型名称。
  List<String> types = [];

  /// The property names of the result set.
  /// 结果集的属性名称。
  List<String> keyNames = [];

  /// The nodes of the result set.
  /// 结果集的节点。
  List<Node> nodes = [];

  /// The links of the result set.
  /// 结果集的关系。
  List<Link> links = [];

  /// The records of the result set.
  /// 结果集的记录。
  List<List<ColVal>> records = [];
}

/// The result of neurodb.
/// neurodb 的结果。
class ResultSet {
  int status = 0;
  int cursor = 0;
  int results = 0;
  int addNodes = 0;
  int addLinks = 0;
  int modifyNodes = 0;
  int modifyLinks = 0;
  int deleteNodes = 0;
  int deleteLinks = 0;
  String? msg;
  RecordSet? recordSet;
}

/// Socket stream reader.
/// Socket 流读取器。
class StringCur {
  late List<int> s;
  late int cur;

  StringCur(String s) {
    try {
      this.s = utf8.encode(s);
    } catch (e) {
      print(e);
    }
    cur = 0;
  }

  String get(int size) {
    Uint8List bytes = Uint8List(size);
    for (int i = 0; i < size; i++) {
      bytes[i] = s[cur + i];
    }
    String subStr = utf8.decode(bytes);
    cur += size;
    return subStr;
  }

  int getType() {
    int type = s[cur];
    cur += 1;
    return type;
  }
}

/// The driver of neurodb.
/// neurodb 的dart驱动。
class NeuroDBDriver {
  late Socket? client = null;
  late String ip;
  late int port;
  late Stream<Uint8List> _btsl;

  /// Create a driver of neurodb.
  /// 创建一个neurodb的dart驱动。
  NeuroDBDriver(this.ip, this.port);

  /// Connect to the server.
  /// 连接到服务器。
  Future<Socket> connect() async {
    var listend = client != null;
    client ??= await Socket.connect(ip, port);
    if (!listend) _btsl = client!.asBroadcastStream();
    return client!;
  }

  /// Close the connection.
  /// 关闭连接。
  close() async {
    return client?.close();
  }

  /// Execute a query.
  /// 执行一个查询。
  Future<ResultSet> executeQuery(String query) async {
    Socket client = await connect();
    client.write(query);

    List<int> bts = [...await _btsl.first];
    ResultSet resultSet = ResultSet();
    var btsstr = utf8.decode([bts.removeAt(0)]);
    var type = btsstr[0];
    if (type == '@') {
      /* 返回的是只有一个成功执行状态位的数据包*/
      resultSet.status = ResultStatus.PARSER_OK.index;
    } else if (type == '\$') {
      /* 返回的是包含错误消息的报错数据包*/
      resultSet.status = ResultStatus.ERROR_INFO.index;
      resultSet.msg = readLine(bts);
    } else if (type == '#') {
      /* 返回的是包含正常消息的消息数据包*/
      resultSet.status = ResultStatus.PARSER_OK.index;
      var msgLen = readLine(bts) ?? '0';
      resultSet.msg = utf8.decode(bts.sublist(0, int.tryParse(msgLen)));
    } else if (type == '*') {
      /* 返回的是图查询结果数据包 */
      var line = readLine(bts);
      var head = line!.split(',');
      resultSet.status = int.parse(head[0]);
      resultSet.cursor = int.parse(head[1]);
      resultSet.results = int.parse(head[2]);
      resultSet.addNodes = int.parse(head[3]);
      resultSet.addLinks = int.parse(head[4]);
      resultSet.modifyNodes = int.parse(head[5]);
      resultSet.modifyLinks = int.parse(head[6]);
      resultSet.deleteNodes = int.parse(head[7]);
      resultSet.deleteLinks = int.parse(head[8]);

      var bodyLen = int.parse(head[9]);
      var body = bts.getRange(0, bodyLen).toList();
      bts.removeRange(0, bodyLen);
      if (resultSet.results > 0) {
        var recordSet = deserializeReturnData(utf8.decode(body));
        resultSet.recordSet = recordSet;
      }
    } else {
      throw Exception("reply type erro");
    }
    return resultSet;
  }

  /// Read type fron [StringCur].
  /// 从 [StringCur] 中读取类型。
  int deserializeType(StringCur cur) {
    return cur.getType();
  }

  /// Read int fron [StringCur].
  /// 从 [StringCur] 中读取int。
  deserializeUint(StringCur cur) {
    var buf = List<int>.filled(3, 0);
    buf[0] = cur.get(1).codeUnitAt(0);
    buf[1] = cur.get(1).codeUnitAt(0);
    buf[2] = cur.get(1).codeUnitAt(0);
    return (buf[0] & 0x7f) << 14 | (buf[1] & 0x7f) << 7 | (buf[2]);
  }

  /// Read string fron [StringCur].
  /// 从 [StringCur] 中读取string。
  String deserializeString(StringCur cur) {
    var len = deserializeUint(cur);
    var val = cur.get(len);
    return val;
  }

  /// Read string list fron [StringCur].
  /// 从 [StringCur] 中读取string list。
  List<String> deserializeStringList(StringCur cur) {
    var len = deserializeUint(cur);
    var list = List<String>.filled(len, '');
    for (var i = 0; i < len; i++) {
      list[i] = deserializeString(cur);
    }
    return list;
  }

  /// Read labels for data from [cur] and [labels].
  /// 从 [cur] 和 [labels] 中读取数据的标签。
  List<String> deserializeLabels(StringCur cur, List<String> labels) {
    var listlen = deserializeUint(cur);
    var l = <String>[];
    while (listlen > 0) {
      var i = deserializeUint(cur);
      l.add(labels[i]);
      listlen--;
    }
    return l;
  }

  /// Read properties for data from [cur] and [keyNames].
  /// 从 [cur] 和 [keyNames] 中读取数据的属性。
  Map<String, ColVal> deserializeKVList(StringCur cur, List<String> keyNames) {
    var listlen = deserializeUint(cur);
    var properties = <String, ColVal>{};
    while (listlen-- > 0) {
      var i = deserializeUint(cur);
      // var key = keyNames[i];
      var type = deserializeUint(cur);
      var aryLen = 0;
      var val = ColVal();
      val.type = type;
      if (type == VO_STRING) {
        val.val = deserializeString(cur);
      } else if (type == VO_NUM) {
        var doubleStr = deserializeString(cur);
        val.val = double.parse(doubleStr);
      } else if (type == VO_STRING_ARRY) {
        aryLen = deserializeUint(cur);
        var ary = [];
        for (var i = 0; i < aryLen; i++) {
          ary.add(deserializeString(cur));
        }
        val.val = ary;
      } else if (type == VO_NUM_ARRY) {
        aryLen = deserializeUint(cur);
        var ary = [];
        for (var i = 0; i < aryLen; i++) {
          var doubleStr = deserializeString(cur);
          ary.add(double.parse(doubleStr));
        }
        val.val = ary;
      } else {
        throw Exception("Error Type");
      }
      properties[keyNames[i]] = val;
    }
    return properties;
  }

  /// Read node from [cur] and [labels] and [keyNames].
  /// 从 [cur] 和 [labels] 和 [keyNames] 中读取节点。
  deserializeCNode(StringCur cur, List<String> labels, List<String> keyNames) {
    var id = deserializeUint(cur);
    var nlabels = deserializeLabels(cur, labels);
    var kvs = deserializeKVList(cur, keyNames);
    var n = Node(id, nlabels, kvs);
    return n;
  }

  /// Read link from [cur] and [types] and [keyNames].
  /// 从 [cur] 和 [types] 和 [keyNames] 中读取关系。
  deserializeCLink(StringCur cur, List<String> types, List<String> keyNames) {
    var id = deserializeUint(cur);
    var hid = deserializeUint(cur);
    var tid = deserializeUint(cur);
    var ty = deserializeType(cur);
    String type = "";
    if (ty == NEURODB_EXIST) {
      var typeIndex = deserializeUint(cur);
      type = types[typeIndex];
    }
    var kvs = deserializeKVList(cur, keyNames);
    var l = Link(id, hid, tid, type, kvs);
    return l;
  }

  /// Get node by id from RecordSet which cached in [deserializeReturnData].
  /// 从 [deserializeReturnData] 中缓存的RecordSet中根据id获取节点。
  getNodeById(List<Node> nodes, id) {
    for (var i = 0; i < nodes.length; i++) {
      if (nodes[i].id == id) {
        return nodes[i];
      }
    }
    return null;
  }

  /// Get link by id from RecordSet which cached in [deserializeReturnData].
  /// 从 [deserializeReturnData] 中缓存的RecordSet中根据id获取关系。
  getLinkById(List<Link> links, id) {
    for (var i = 0; i < links.length; i++) {
      if (links[i].id == id) {
        return links[i];
      }
    }
    return null;
  }

  /// Read RecordSet from [body].
  /// 从 [body] 中读取RecordSet。
  RecordSet deserializeReturnData(String body) {
    StringCur cur = StringCur(body);
    RecordSet rd = RecordSet();
    List? path = null;
    /*读取labels、types、keyNames列表*/
    if (deserializeType(cur) != NEURODB_RETURNDATA) {
      throw Exception("Error Type");
    }
    rd.labels = deserializeStringList(cur);
    rd.types = deserializeStringList(cur);
    rd.keyNames = deserializeStringList(cur);
    /*读取节点列表*/
    if (deserializeType(cur) != NEURODB_NODES) throw Exception("Error Type");
    int cntNodes;
    cntNodes = deserializeUint(cur);
    for (int i = 0; i < cntNodes; i++) {
      Node n = deserializeCNode(cur, rd.labels, rd.keyNames);
      rd.nodes.add(n);
    }
    /*读取关系列表*/
    if (deserializeType(cur) != NEURODB_LINKS) throw Exception("Error Type");
    int cntLinks;
    cntLinks = deserializeUint(cur);
    for (int i = 0; i < cntLinks; i++) {
      Link l = deserializeCLink(cur, rd.types, rd.keyNames);
      rd.links.add(l);
    }
    /*读取return结果集列表*/
    if (deserializeType(cur) != NEURODB_RECORDS) throw Exception("Error Type");
    int cntRecords;
    cntRecords = deserializeUint(cur);
    for (int i = 0; i < cntRecords; i++) {
      int type, cntColumn;
      if (deserializeType(cur) != NEURODB_RECORD) throw Exception("Error Type");
      cntColumn = deserializeUint(cur);
      List<ColVal> record = [];
      for (int j = 0; j < cntColumn; j++) {
        int aryLen = 0;
        type = deserializeType(cur);
        ColVal val = ColVal();
        val.type = type;
        if (type == NEURODB_NIL) {
          /*val =NULL;*/
        } else if (type == VO_NODE) {
          int id;
          id = deserializeUint(cur);
          Node n = getNodeById(rd.nodes, id);
          val.val = n;
        } else if (type == VO_LINK) {
          int id;
          id = deserializeUint(cur);
          Link l = getLinkById(rd.links, id);
          val.val = l;
        } else if (type == VO_PATH) {
          int len;
          len = deserializeUint(cur);
          path = [];
          for (i = 0; i < len; i++) {
            int id;
            id = deserializeUint(cur);
            if (i % 2 == 0) {
              Node nd = getNodeById(rd.nodes, id);
              path.add(nd);
            } else {
              Link lk = getLinkById(rd.links, id);
              path.add(lk);
            }
          }
          val.val = path;
        } else if (type == VO_STRING) {
          val.val = deserializeString(cur);
        } else if (type == VO_NUM) {
          String doubleStr = deserializeString(cur);
          val.val = double.parse(doubleStr);
        } else if (type == VO_STRING_ARRY) {
          aryLen = deserializeUint(cur);
          List<String> valAry = List.filled(aryLen, "");
          for (i = 0; i < aryLen; i++) {
            valAry[i] = deserializeString(cur);
          }
          val.val = valAry;
        } else if (type == VO_NUM_ARRY) {
          aryLen = deserializeUint(cur);
          List<double> valAry = List.filled(aryLen, 0);
          for (i = 0; i < aryLen; i++) {
            String doubleStr = deserializeString(cur);
            valAry[i] = double.parse(doubleStr);
          }
          val.val = valAry;
        } else {
          throw Exception("Error Type");
        }
        record.add(val);
      }
      rd.records.add(record);
    }
    /*读取结束标志*/
    if (deserializeType(cur) != NEURODB_EOF) throw Exception("Error Type");
    return rd;
  }
}

/// Read a string from [bts]. It seems that strings split by '\n' in [bts].
/// 从 [bts] 中读取一个字符串。看起来像 [bts] 中的字符串以 '\n' 分隔。
String? readLine(List<int> bts) {
  String sb = '';

  while (bts.isNotEmpty) {
    var btssr = utf8.decode([bts.removeAt(0)]);
    dynamic c = btssr[0];
    sb = sb + c;
    if (c == '\n') {
      break;
    }
    btssr = utf8.decode(bts);
    c = btssr[0];
  }

  return sb.replaceAll('\r\n', '');
}

// 状态码新增时，请勿打乱原有顺序。
/// 结果状态码
enum ResultStatus {
  /// 错误消息
  ERROR_INFO,

  /// 运行成功
  PARSER_OK,

  /// 内存分配异常
  NO_MEM_ERR,

  /// 普通语法错误
  SYNTAX_ERR,

  /// 未找到此指令
  NO_Exp_ERR,

  /// 缺少关系
  NO_LNK_ERR,

  /// 缺少箭头
  NO_ARROW_ERR,

  /// 关系双箭头错误
  DOU_ARROW_ERR,

  /// 缺少头节点
  NO_HEAD_ERR,

  /// 缺少尾结点
  NO_TAIL_ERR,

  /// 必须是字母数字下划线
  CHAR_NUM_UL_ERR,

  /// 不是模式表达式
  NOT_PATTERN_ERR,

  /// 此变量已被使用
  DUP_VAR_NM_ERR,

  /// 数组中含有不相同的类型
  NO_SM_TYPE_ERR,

  /// 不支持的数据类型
  NO_SUP_TYPE,

  /// 指令搭配错误
  WRON_EXP,

  /// 暂不支持的指令
  NOT_SUPPORT,

  /// where语句语法
  WHERE_SYN_ERR,

  /// where运算语法
  WHERE_RUN_ERR,

  /// 未找到变量
  NO_VAR_ERR,

  /// 缺失配对括号
  NO_PAIR_BRK,

  /// 删除带有关边的节点
  CLIST_HAS_LINK_ERR,

  /// 数据操作错误
  CLIST_OPR_ERR,

  /// order by语句语法
  ORDER_BY_SYN_ERR,

  /// 不可删除路径
  DEL_PATH_ERR,

  /// 未定义的变量
  UNDEFINED_VAR_ERR,

  /// where 模式条件，独立连通图缺少变量
  WHERE_PTN_NO_VAR_ERR,

  /// 不支持的存储过程
  NO_PROC_ERR,

  /// csv文件读取错误
  CSV_FILE_ERR,

  /// csv 变量属性名在列中未找到
  CSV_ROW_VAR_ERR,

  /// 查找过载超时
  QUERY_TIME_OUT_ERR;
}

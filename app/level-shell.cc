#include <iostream>
#include <memory>
#include <vector>

#include "leveldb/db.h"

using std::cin;
using std::cout;
using std::endl;
using std::string;

void PrintHelp() { cout << "usage: level-shell ${leveldb path}" << endl; }

// 分割字符串，返回参数数组
std::vector<string> Split(const string& s, char c = ' ') {
  if (s.empty()) return {};
  std::vector<string> res;
  string::size_type pos = s.find(c), prev = 0;
  while (pos != string::npos) {
    if (pos != prev) {
      res.emplace_back(begin(s) + prev, begin(s) + pos);
    }
    prev = pos + 1;
    pos = s.find(c, prev);
  }
  if (prev < s.size() && s[prev] != c) {
    res.emplace_back(begin(s) + prev, end(s));
  }
  return res;
}

// 提取 scan 参数中引号里的 Key
leveldb::Status ParseScanParameters(const string& s, string* param) {
  if (s.size() < 2 || s.front() != s.back() ||
      s.front() != '"' && s.front() != '\'') {
    return leveldb::Status::InvalidArgument(s,
                                            "should be formatted like \"xxx\"");
  }
  *param = s.substr(1, s.size() - 2);
  return leveldb::Status::OK();
}

int main(int argc, char** argv) {

  // 参数校验，输出提示信息
  if (argc != 2) {
    PrintHelp();
    exit(-1);
  }

  // 读取第二个参数，即要打开的 DB
  leveldb::DB* db_ptr;
  leveldb::Options opt;
  opt.create_if_missing = true;
  auto status = leveldb::DB::Open(opt, argv[1], &db_ptr);
  if (!status.ok()) {
    cout << status.ToString() << endl;
    exit(-1);
  }

  // 为 DB 创建智能指针
  auto db = std::shared_ptr<leveldb::DB>(db_ptr);

  cout << ">>> " << std::flush;
  
  // 解析命令，读取一整行输入
  string command;
  while (getline(cin, command)) {

    // 将命令进行分割，得到字符串数组
    auto commands = Split(command);
    
    // 校验参数
    if (commands.empty()) {
      goto PREFIX;
    }

    // 判断要进行的操作，并校验相应操作的参数长度
    if (commands[0] == "put" && commands.size() == 3) {
      status = db->Put({}, commands[1], commands[2]);
      cout << status.ToString() << endl;
    } else if (commands[0] == "get" && commands.size() == 2) {
      string value;
      status = db->Get({}, commands[1], &value);
      if (!status.ok()) {
        cout << status.ToString() << endl;
      }
      cout << value << endl;
    } else if (commands[0] == "scan" && commands.size() == 3) {
      
      std::string start, end;
      // 为 scan 操作迭代器创建共享指针
      auto iter = std::shared_ptr<leveldb::Iterator>(db->NewIterator({}));
      iter->Seek(start);
      while (iter->Valid() && iter->status().ok()) {

        // 提取 scan 操作相应的起始和结束参数
        status = ParseScanParameters(commands[1], &start);
        if (!status.ok()) {
          cout << status.ToString() << endl;
          break;
        }
        status = ParseScanParameters(commands[2], &end);
        if (!status.ok()) {
          cout << status.ToString() << endl;
          break;
        }

        // scan 操作结束处理（查询到范围的边界）
        if (!end.empty() && iter->key().compare(end) >= 0) {
          break;
        }
        cout << iter->key().ToString() << ":" << iter->value().ToString()
             << endl;
        iter->Next();
      }

      // 获取迭代器失败，输出错误信息
      if (!iter->status().ok()) {
        cout << iter->status().ToString() << endl;
      }
    } else if (commands[0] == "delete" && commands.size() == 2) {
      status = db->Delete({}, commands[1]);
      if (!status.ok()) {
        cout << status.ToString() << endl;
      }
    } else {

      // 暂时只支持一些基本操作（get/put/delete/scan）
      cout << "invalid operate" << endl;
    }
  
  // 错误处理 goto
  PREFIX:
    cout << ">>> " << std::flush;
  }

  return 0;
}
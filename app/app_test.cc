#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include <cassert>
#include <iostream>

using namespace std;
using namespace leveldb;

int main() {

    // Open DB
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "testdb", &db);
    assert(status.ok());

    // Put
    status = db->Put(WriteOptions(), "KeyNameExample", "ValueExample");
    assert(status.ok());

    // Get
    string res;
    status = db->Get(ReadOptions(), "KeyNameExample", &res);
    assert(status.ok());
    cout << res << endl;

    // Write
    leveldb::WriteBatch batch;
    batch.Delete("KeyNameExample");
    batch.Put("NewKeyNameExample", "NewValueExample");
    status = db->Write(WriteOptions(), &batch);
    assert(status.ok());

    // Scan
    leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        cout << it->key().ToString() << ": "  << it->value().ToString() << endl;
    }
    assert(it->status().ok());  // Check for any errors found during the scan
    delete it;

    delete db;
    return 0;
}
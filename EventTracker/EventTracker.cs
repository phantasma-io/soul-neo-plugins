using Neo.IO.Json;
using Neo.Ledger;
using Neo.VM;
using System;
using System.Collections.Generic;
using Snapshot = Neo.Persistence.Snapshot;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Text;
using System.Linq;
using Neo.Wallets;
using Microsoft.AspNetCore.Http;
using Neo.Network.P2P.Payloads;

namespace Neo.Plugins
{
    public class EventTracker: Plugin, IRpcPlugin, IPersistencePlugin
    {
        private readonly MongoClient client;

        public override string Name => "EventTracker";

        public EventTracker()
        {
            var s = Settings.Default;
            if (s.MongoUser != "")
            {
                client = new MongoClient($"mongodb://{s.MongoUser}:{s.MongoPass}@{s.MongoHost}:{s.MongoPort}");
                Console.WriteLine($"connected to mongodb://xxx:xxx@{s.MongoHost}:{s.MongoPort}");
            }
            else
            {
                client = new MongoClient($"mongodb://{s.MongoHost}:{s.MongoPort}");
                Console.WriteLine($"connected to mongodb://{s.MongoHost}:{s.MongoPort}");
            }
        }

        public override void Configure()
        {
            Settings.Load(GetConfiguration());

            foreach (ContractConfig contract in Settings.Default.ContractConfigs)
            {
                foreach (MongoAction action in contract.Actions)
                {
                    Console.WriteLine($"Monitoring {contract.Scripthash} for {action.OnNotification} notification");
                }
            }
        }

        public void PreProcess(HttpContext context, string method, JArray _params)
        {
        }

        public JObject OnProcess(HttpContext context, string method, JArray _params)
        {
            if (method == "getcontractlog")
            {
                return HandleGetContractLog(_params);
            }
            else if (method == "getblockids")
            {
                return HandleGetBlockIds(_params);
            }
            else
            {
                return null;
            }
        }

        public void PostProcess(HttpContext context, string method, JArray _params, JObject result)
        {
        }

        private JObject HandleGetBlockIds(JArray _params)
        {
            string hash = _params[0].AsString();
            string address = _params[1].AsString();
            ulong height = 0;

            try
            {
                height = (ulong)_params[2].AsNumber();
            }
            catch
            {
                height = 0;
            }

            var db = client.GetDatabase(hash);
            if (db == null) 
            {
                return JObject.Parse("database not available");
            }

            IList<FilterDefinition<BsonDocument>> filters = new List<FilterDefinition<BsonDocument>>();
            IList<FilterDefinition<BsonDocument>> addressFilters = new List<FilterDefinition<BsonDocument>>();
            var collection = db.GetCollection<BsonDocument>("transfers");
            var builder = Builders<BsonDocument>.Filter;

            if (height > 0)
            {
                filters.Add(builder.Gte("block_index", height));
            }

            addressFilters.Add(builder.Eq("to_address", address));
            addressFilters.Add(builder.Eq("from_address", address));
            filters.Add(builder.Or(addressFilters));

            var filter = builder.And(filters);
            var sort = Builders<BsonDocument>.Sort.Ascending("block_index");
            //var blockIds = collection.Distinct<ulong>("block_index", filter);//.ToDictionary(x => x.).OrderBy(x => x);
            var blockIds = new Dictionary<ulong,string>();

            collection.Aggregate().Group(new BsonDocument("_id", new BsonDocument {
                                                                                        {"block_index", "$block_index"},
                                                                                        {"block_hash", "$block_hash"}
                                                                                  })).ToList().ForEach(
                doc =>  { 
                            var data = doc["_id"];
                            blockIds.Add((ulong)data["block_index"].AsInt32, data["block_hash"].AsString);
                        });

            var objects = new JArray();
            foreach (var entry in blockIds)
            {
                JObject item = new JObject();
                item["block_index"] = entry.Key;
                item["block_hash"] = entry.Value;

                objects.Add(item);
            }

            return objects;
        }

        private JObject HandleGetContractLog(JArray _params)
        {
            uint mode = (uint)_params[0].AsNumber();
            string hash = _params[1].AsString();
            string address = _params[2].AsString();
            ulong height = 0;

            try
            {
                height = (ulong)_params[3].AsNumber();
            }
            catch
            {
                height = 0;
            }

            var db = client.GetDatabase(hash);
            if (db == null) 
            {
                return JObject.Parse("database not available");
            }

            IList<FilterDefinition<BsonDocument>> filters = new List<FilterDefinition<BsonDocument>>();
            IList<FilterDefinition<BsonDocument>> addressFilters = new List<FilterDefinition<BsonDocument>>();
            var collection = db.GetCollection<BsonDocument>("transfers");
            var builder = Builders<BsonDocument>.Filter;

            if (mode == 1)
            {
                filters.Add(builder.Eq("to_address", address));
            }
            else if (mode == 2)
            {
                filters.Add(builder.Eq("from_address", address));
            }
            else if (mode == 3)
            {
                addressFilters.Add(builder.Eq("to_address", address));
                addressFilters.Add(builder.Eq("from_address", address));
                filters.Add(builder.Or(addressFilters));
            }
            else
            {
                return JObject.Parse("Unknown mode!");
            }

            if (height > 0)
            {
                filters.Add(builder.Gte("block_index", height));
            }

            var filter = builder.And(filters);
            // to debug
            //var documentSerializer = BsonSerializer.SerializerRegistry.GetSerializer<BsonDocument>();
            //var renderedFilter = filter.Render(documentSerializer, BsonSerializer.SerializerRegistry).ToString();
            //Console.WriteLine("filter: ");
            //Console.WriteLine(renderedFilter);

            var sort = Builders<BsonDocument>.Sort.Ascending("block_index");
            var transfers = collection.Find(filter).Sort(sort).ToList();

            var objects = new JArray();
            try 
            {
                foreach (var transfer in transfers)
                {
                    JObject item = new JObject();
                    item["from_address"] = transfer.GetValue("from_address").ToString();
                    item["to_address"] = transfer.GetValue("to_address").ToString();
                    item["amount"] = transfer.GetValue("amount").ToString();
                    item["txid"] = transfer.GetValue("txid").ToString();
                    item["block_index"] = transfer.GetValue("block_index").ToString();
                    item["block_hash"] = transfer.GetValue("block_hash").ToString();

                    JObject attributes = JObject.Parse(transfer.GetValue("attributes").ToString());
                    item["attributes"] = attributes;

                    objects.Add(item);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            return objects;
        }

        public void OnPersist(Snapshot snapshot, IReadOnlyList<Blockchain.ApplicationExecuted> applicationExecutedList)
        {
            ulong height = snapshot.PersistingBlock.Index;
            UInt256 hash = snapshot.PersistingBlock.Hash;
            var db = client.GetDatabase("chaindata");
            var collection = db.GetCollection<BsonDocument>("chaindata");
            BsonDocument doc = BsonDocument.Parse("{\"chain\": 1, \"currentHeight\":" + height.ToString() + "}");
            var filter = Builders<BsonDocument>.Filter.Eq("chain", 1);
            UpdateOptions options = new UpdateOptions();
            options.IsUpsert = true;
            collection.ReplaceOne(filter, doc, options);

            foreach (var appExec in applicationExecutedList)
            {
                var attributes = appExec.Transaction.Attributes;
                var txid = appExec.Transaction.Hash.ToString();
                foreach (ApplicationExecutionResult p in appExec.ExecutionResults)
                {
                    if (p.VMState.ToString().Contains("HALT"))
                    {
                        foreach (SmartContract.NotifyEventArgs q in p.Notifications)
                        {
                            var contract = q.ScriptHash.ToString();

                            foreach (ContractConfig h in Settings.Default.ContractConfigs)
                            {
                                if (contract.Contains(h.Scripthash))
                                {
                                    JObject notification = q.State.ToParameter().ToJson();
                                    HandleNotification(height, hash.ToString(), attributes, txid, h.Scripthash, notification["value"] as JArray, h.Actions);
                                }
                            }
                        }
                    }
                }
            }
        }

        private void HandleNotification(ulong height, string blockHash, TransactionAttribute[] attributes, string txid, string Scripthash, JArray notificationFields, MongoAction[] actions)
        {
            string notification_type = Encoding.UTF8.GetString(notificationFields[0]["value"].AsString().HexToBytes());
            foreach (MongoAction a in actions)
            {
                if (a.OnNotification == notification_type)
                {
                    JObject record = new JObject();
                    JArray attr = new JArray();

                    foreach (var attribute in attributes)
                    {
                        attr.Add(attribute.ToJson());
                    }

                    string idxKey = "";
                    int count = 1;
                    foreach (Dictionary<string, string> s in a.Schema)
                    {
                        if (s["Type"] == "string")
                        {
                            record[s["Name"]] = Encoding.UTF8.GetString(notificationFields[count]["value"].AsString().HexToBytes());
                        }
                        else if (s["Type"] == "integer")
                        {
                            if (notificationFields[count]["type"].AsString() == "Integer")
                            {
                                record[s["Name"]] = notificationFields[count]["value"].AsNumber();
                            }
                            else
                            {
                                record[s["Name"]] = HexToInt(notificationFields[count]["value"].AsString());
                            }
                        }
                        else if (s["Type"].Contains("uint"))
                        {
                            record[s["Name"]] = notificationFields[count]["value"].AsString().HexToBytes().Reverse().ToHexString();
                        }
                        else if (s["Type"] == "address")
                        {
                            record[s["Name"]] = UInt160.Parse(notificationFields[count]["value"].AsString().HexToBytes().Reverse().ToHexString()).ToAddress();
                        }
                        else
                        {
                            record[s["Name"]] = notificationFields[count]["value"].AsString();
                        }

                        if (count == a.Keyindex)
                        {
                            idxKey = record[s["Name"]].AsString();
                        }
                        count += 1;
                    }

                    if (a.Keyindex > 0)
                    {
                        record["refid"] = idxKey;
                    }

                    record["txid"] = txid;
                    record["block_index"] = height;
                    record["block_hash"] = blockHash;
                    record["attributes"] = attr;

                    BsonDocument doc = BsonDocument.Parse(record.ToString());
                    var db = client.GetDatabase(Scripthash);
                    var collection = db.GetCollection<BsonDocument>(a.Collection);
                    if (a.Action == "create")
                    {
                        collection.InsertOne(doc);
                    }
                    else if (a.Action == "delete")
                    {
                        var filter = Builders<BsonDocument>.Filter.Eq("refid", idxKey);
                        collection.DeleteOne(filter);
                    }
                }
            }
        }

        private ulong HexToInt(string hex)
        {
            return BitConverter.ToUInt64(hex.PadRight(16, '0').HexToBytes(), 0);
        }

        public void OnCommit(Snapshot snapshot)
        {
        }

        public bool ShouldThrowExceptionFromCommit(Exception ex)
        {
            return false;
        }
    }
}

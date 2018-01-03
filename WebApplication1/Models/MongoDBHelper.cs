using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Configuration;

namespace WebApplication1.Models
{
    public class MongoDBHelper
    {

        /// <summary>  
        /// 数据库所在主机  
        /// </summary>  
        private readonly string MONGO_CONN_HOST;

        /// <summary>  
        /// 数据库所在主机的端口  
        /// </summary>  
        private readonly int MONGO_CONN_PORT = 27017;

        /// <summary>  
        /// 连接超时设置 15秒  
        /// </summary>  
        private readonly int CONNECT_TIME_OUT = 15;

        /// <summary>  
        /// 数据库的名称  
        /// </summary>  
        private string DB_NAME;


        /// <summary>  
        /// 数据库的实例  
        /// </summary>  
        private IMongoDatabase _DB;

        /// <summary>
        /// Mongo客户端实例
        /// </summary>
        private IMongoClient client = null;

        public string DatabaseName
        {
            get { return DB_NAME; }
            set
            {
                DB_NAME = value;
                _DB = client.GetDatabase(DB_NAME);
            }
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="connectionString">MongoDB连接路径</param>
        /// <param name="databaseName">数据库名</param>
        public MongoDBHelper(string connectionString, string databaseName)
        {
            this.DB_NAME = databaseName;
            client = new MongoClient(connectionString);
            _DB = client.GetDatabase(databaseName);
        }
        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="connectionString">MongoDB连接路径</param>
        public MongoDBHelper(string connectionString)
        {
            client = new MongoClient(connectionString);
        }


        /// <summary>
        /// 执行命令，命令请参考MongoCommand,命令太多，不一一展示，传入的就是里面的字符串，有些命令执行需要连接到admin表
        /// </summary>
        /// <param name="cmdText"></param>
        /// <returns></returns>
        public BsonDocument RunCommand(string cmdText)
        {
            return _DB.RunCommand<BsonDocument>(cmdText);
        }

        public IList<BsonDocument> GetDatabase()
        {
            return client.ListDatabases().ToList();
        }


        ///// <summary>  
        ///// 得到数据库实例  
        ///// </summary>  
        ///// <returns></returns>  
        //public IMongoDatabase GetDataBase()
        //{
        //    MongoClientSettings mongoSetting = new MongoClientSettings();
        //    //设置连接超时时间  
        //    mongoSetting.ConnectTimeout = new TimeSpan(CONNECT_TIME_OUT * TimeSpan.TicksPerSecond);
        //    //设置数据库服务器  
        //    mongoSetting.Server = new MongoServerAddress(MONGO_CONN_HOST, MONGO_CONN_PORT);
        //    //创建Mongo的客户端  
        //    MongoClient client = new MongoClient(CONN_STR);

        //    //得到服务器端并且生成数据库实例  
        //    return client.GetDatabase(DB_NAME);
        //}


        #region 插入数据  
        /// <summary>
        /// 插入数据
        /// </summary>
        /// <typeparam name="T">插入类型</typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="document">插入实例</param>
        public void Insert<T>(string collectionName, T document)
        {
            try
            {
                _DB.GetCollection<T>(collectionName).InsertOne(document);
            }
            catch (MongoWriteException me)
            {
                MongoBulkWriteException mbe = me.InnerException as MongoBulkWriteException;
                if (mbe != null && mbe.HResult == -2146233088)
                    throw new Exception("插入重复的键！");
                throw new Exception(mbe.Message);
            }
            catch (Exception ep)
            {
                throw ep;
            }
        }

        /// <summary>
        /// 插入多个文档
        /// </summary>
        /// <typeparam name="T">类型</typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="documents">插入实例集合</param>
        public void InsertMany<T>(string collectionName, IList<T> documents)
        {
            try
            {
                _DB.GetCollection<T>(collectionName).InsertMany(documents);
            }
            catch (MongoWriteException me)
            {
                MongoBulkWriteException mbe = me.InnerException as MongoBulkWriteException;
                if (mbe != null && mbe.HResult == -2146233088)
                    throw new Exception("插入重复的键！");
                throw new Exception(mbe.Message);
            }
            catch (Exception ep)
            {
                throw ep;
            }
        }
        #endregion

        #region 查询数据
        /// <summary>
        /// 判断文档存在状态
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="filterexist"></param>
        /// <returns></returns>
        public bool IsExistDocument<T>(string collectionName, FilterDefinition<T> filter)
        {
            return _DB.GetCollection<T>(collectionName).Count(filter) > 0;
        }

        /// <summary>
        /// 通过条件得到查询的结果个数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public long GetCount<T>(string collectionName, FilterDefinition<T> filter)
        {
            return _DB.GetCollection<T>(collectionName).Count(filter);
        }

        /// <summary>
        /// 通过系统id(ObjectId)获取一个对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="id"></param>
        /// <returns></returns>
        public T GetDocumentById<T>(string collectionName, string id)
        {
            ObjectId oid = ObjectId.Parse(id);
            var filter = Builders<T>.Filter.Eq("_id", oid);
            var result = _DB.GetCollection<T>(collectionName).Find(filter);
            return result.FirstOrDefault();
        }

        /// <summary>
        /// 通过系统id(ObjectId)获取一个对象同时过滤字段
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="id"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public T GetDocumentById<T>(string collectionName, string id, ProjectionDefinition<T> fields)
        {
            ObjectId oid = ObjectId.Parse(id);
            var filter = Builders<T>.Filter.Eq("_id", oid);
            return _DB.GetCollection<T>(collectionName).Find(filter).Project<T>(fields).FirstOrDefault();
        }

        /// <summary>
        /// 通过指定的条件获取一个对象，如果有多条，只取第一条，同时过滤字段
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="filter"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        public T GetDocumentByUserFilter<T>(string collectionName, FilterDefinition<T> filter, ProjectionDefinition<T> fields)
        {
            return _DB.GetCollection<T>(collectionName).Find(filter).Project<T>(fields).FirstOrDefault();
        }

        /// <summary>
        /// 获取全部文档
        /// </summary>
        /// <typeparam name="T"></typeparam>       
        /// <param name="collectionName">集合名</param>
        /// <returns></returns>
        public IList<T> GetAllDocuments<T>(string collectionName)
        {
            var filter = Builders<T>.Filter.Empty;
            return _DB.GetCollection<T>(collectionName).Find(filter).ToList();
        }


        /// <summary>
        /// 获取全部文档同时过滤字段
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="fields">要获取的字段</param>
        /// <returns></returns>
        public IList<T> GetAllDocuments<T>(string collectionName, ProjectionDefinition<T> fields)
        {
            var filter = Builders<T>.Filter.Empty;
            return _DB.GetCollection<T>(collectionName).Find(filter).Project<T>(fields).ToList();
        }

        /// <summary>
        /// 通过一个条件获取对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="property">字段名</param>
        /// <param name="value">字段值</param>
        /// <returns></returns>
        public IList<T> GetDocumentsByFilter<T>(string collectionName, string property, string value)
        {
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(property, value);
            return _DB.GetCollection<T>(collectionName).Find(filter).ToList();
        }

        /// <summary>
        /// 通过条件获取对象
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="filter"></param>
        /// <returns></returns>
        public IList<T> GetDocumentsByFilter<T>(string collectionName, FilterDefinition<T> filter)
        {
            return _DB.GetCollection<T>(collectionName).Find(filter).ToList();
        }

        /// <summary>
        /// 通过条件获取对象,同时过滤字段
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="property">字段名</param>
        /// <param name="value">字段值</param>
        /// <param name="fields">要获取的字段</param>
        /// <returns></returns>
        public IList<T> GetDocumentsByFilter<T>(string collectionName, string property, string value, ProjectionDefinition<T> fields)
        {
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(property, value);
            return _DB.GetCollection<T>(collectionName).Find(filter).Project<T>(fields).ToList();
        }

        /// <summary>
        /// 通过条件获取对象,同时过滤数据和字段
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="filter">过滤器</param>
        /// <param name="fields">要获取的字段</param>
        /// <returns></returns>
        public IList<T> GetDocumentsByFilter<T>(string collectionName, FilterDefinition<T> filter, ProjectionDefinition<T> fields)
        {
            return _DB.GetCollection<T>(collectionName).Find(filter).Project<T>(fields).ToList();
        }

        /// <summary>
        /// 通过条件获取分页的文档并排序
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="filter">过滤器</param>
        /// <param name="fields">要获取的字段</param>
        /// <param name="sort"></param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        public IList<T> GetPagedDocumentsByFilter<T>(string collectionName, FilterDefinition<T> filter, ProjectionDefinition<T> fields, SortDefinition<T> sort, int pageIndex, int pageSize)
        {
            IList<T> result = new List<T>();
            if (pageIndex != 0 && pageSize != 0)
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).Project<T>(fields).Sort(sort).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).ToList();
            }
            else
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).Project<T>(fields).Sort(sort).ToList();
            }
            return result;
        }

        /// <summary>
        /// 通过条件获取分页的文档并排序
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="filter">过滤器</param>
        /// <param name="sort"></param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        public IList<T> GetPagedDocumentsByFilter<T>(string collectionName, FilterDefinition<T> filter, SortDefinition<T> sort, int pageIndex, int pageSize)
        {
            IList<T> result = new List<T>();
            if (pageIndex != 0 && pageSize != 0)
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).Sort(sort).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).ToList();
            }
            else
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).Sort(sort).ToList();
            }
            return result;
        }

        /// <summary>
        /// 通过条件获取分页的文档
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="filter">过滤器</param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        public IList<T> GetPagedDocumentsByFilter<T>(string collectionName, FilterDefinition<T> filter, int pageIndex, int pageSize)
        {
            IList<T> result = new List<T>();
            if (pageIndex != 0 && pageSize != 0)
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).ToList();
            }
            else
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).ToList();
            }
            return result;
        }

        /// <summary>
        /// 获取分页的文档
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        public IList<T> GetPagedDocumentsByFilter<T>(string collectionName, SortDefinition<T> sort, int pageIndex, int pageSize)
        {
            IList<T> result = new List<T>();
            var filter = Builders<T>.Filter.Empty;
            if (pageIndex != 0 && pageSize != 0)
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).Sort(sort).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).ToList();
            }
            else
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).Sort(sort).ToList();
            }
            return result;
        }

        /// <summary>
        /// 获取分页的文档
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="pageIndex"></param>
        /// <param name="pageSize"></param>
        /// <returns></returns>
        public IList<T> GetPagedDocumentsByFilter<T>(string collectionName, int pageIndex, int pageSize)
        {
            IList<T> result = new List<T>();
            var filter = Builders<T>.Filter.Empty;
            if (pageIndex != 0 && pageSize != 0)
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).Skip(pageSize * (pageIndex - 1)).Limit(pageSize).ToList();
            }
            else
            {
                result = _DB.GetCollection<T>(collectionName).Find(filter).ToList();
            }
            return result;
        }

        #endregion

        #region 修改数据
        /// <summary>
        /// 修改单个文档
        /// </summary>
        /// <typeparam name="T">文档类型</typeparam>
        /// <param name="collectionName">集合名</param>
        /// <param name="id">对象id</param>
        /// <param name="oldinfo">需要修改的文档实例</param>
        public void UpdateReplaceOne<T>(string collectionName, string id, T oldinfo)
        {
            ObjectId oid = ObjectId.Parse(id);
            var filter = Builders<T>.Filter.Eq("_id", oid);
            _DB.GetCollection<T>(collectionName).ReplaceOne(filter, oldinfo);
        }

        /// <summary>
        /// 只能替换一条，如果有多条的话
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="filter"></param>
        /// <param name="oldinfo"></param>
        public void UpdateReplaceOne<T>(string collectionName, FilterDefinition<T> filter, T oldinfo)
        {
            _DB.GetCollection<T>(collectionName).ReplaceOne(filter, oldinfo);
        }

        /// <summary>
        /// 更新指定属性值，按ID就只有一条，替换一条
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="documentname"></param>
        /// <param name="id"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        public void Update<T>(string collectionName, string id, string property, string value)
        {
            ObjectId oid = ObjectId.Parse(id);
            var filter = Builders<T>.Filter.Eq("_id", oid);
            var update = Builders<T>.Update.Set(property, value);
            _DB.GetCollection<T>(collectionName).UpdateOne(filter, update);
        }

        public void Update<T>(string collectionName, FilterDefinition<T> filter, UpdateDefinition<T> update)
        {
            _DB.GetCollection<T>(collectionName).UpdateOne(filter, update);
        }

        public void UpdateMany<T>(string collectionName, FilterDefinition<T> filter, UpdateDefinition<T> update)
        {
            _DB.GetCollection<T>(collectionName).UpdateMany(filter, update);
        }
        #endregion

        #region 删除数据
        /// <summary>
        /// 删除一个文档
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="id"></param>
        public void Delete<T>(string collectionName, string id)
        {
            ObjectId oid = ObjectId.Parse(id);
            var filterid = Builders<T>.Filter.Eq("_id", oid);
            _DB.GetCollection<T>(collectionName).DeleteOne(filterid);
        }

        public void Delete<T>(string collectionName, string property, string value)
        {
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(property, value);
            _DB.GetCollection<T>(collectionName).DeleteOne(filter);
        }

        /// <summary>
        /// 通过一个属性名和属性值删除多个文档
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="property"></param>
        /// <param name="value"></param>
        public void DeleteMany<T>(string collectionName, string property, string value)
        {
            FilterDefinition<T> filter = Builders<T>.Filter.Eq(property, value);
            _DB.GetCollection<T>(collectionName).DeleteMany(filter);
        }

        /// <summary>
        /// 通过过滤器删除多个文档 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="collectionName"></param>
        /// <param name="filter"></param>
        public void DeleteMany<T>(string collectionName, FilterDefinition<T> filter)
        {
            _DB.GetCollection<T>(collectionName).DeleteMany(filter);
        }

        #endregion

    }
}
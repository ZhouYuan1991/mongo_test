using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using WebApplication1.Models;

namespace WebApplication1.Controllers
{
    public class HomeController : Controller
    {
        MongoDBHelper mongo_conn = new MongoDBHelper("mongodb://127.0.0.1:27017", "zy");
        public ActionResult Index()
        {

            //Dictionary<string, int> user_map = new Dictionary<string, int>();
            //user_map.Add("litong",20);
            //user_map.Add("xiaohuanghuang", 21);
            //user_map.Add("zy", 22);
            //user_map.Add("jly", 23);
            //user_map.Add("zhangsan", 2);
            //InsertTestMethod();
            IList<User> get_result = mongo_conn.GetAllDocuments<User>("user");
            foreach (var item in get_result)
            {
                Console.WriteLine(item);
            }
            return View();
        }

        public void InsertTestMethod()
        {
            for (int i = 0; i < 10; i++)
            {
                User te = GetEntity();
                mongo_conn.Insert<User>("user", te);
            }
        }
        /// <summary>
        /// 获取User的实体
        /// </summary>
        /// <returns></returns>
        private User GetEntity()
        {
            Random rad = new Random();
            User te = new User();
            te.OrderId = Guid.NewGuid().ToString("N");
            te.CreateDate = DateTime.Now;
            te.CustomerId = rad.Next(100).ToString();
            te.CustomerName = rad.Next(100).ToString();
            te.Note = rad.Next(100).ToString();
            te.Qty = rad.Next(100);
            te.OrderDate = DateTime.Now;
            te.OrderName = rad.Next(100).ToString();
            return te;
        }

        public ActionResult About()
        {
            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Your contact page.";

            return View();
        }
    }
}
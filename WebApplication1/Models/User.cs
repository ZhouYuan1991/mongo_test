using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using MongoDB.Bson;

namespace WebApplication1.Models
{
    public class User
    {
        public ObjectId Id { get; set; }
        public string OrderId { get; set; }
        public string OrderName { get; set; }
        public DateTime OrderDate { get; set; }
        public string CustomerId { get; set; }
        public string CustomerName { get; set; }
        public int Qty { get; set; }
        public string Note { get; set; }
        public DateTime CreateDate { get; set; }
    }
}
using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace ChangeFeedMaterialisedView
{
    public class CartItem
    {
         public string Id { get; set; }
        
         [JsonConverter(typeof(StringEnumConverter))]
         public HomeServices Service{ get; set; }

         public decimal UnitPrice { get; set; }
    }
}

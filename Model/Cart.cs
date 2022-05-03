using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace ChangeFeedMaterialisedView
{
    public class Cart
    {
        public Cart()
        {

        }
        [JsonProperty(PropertyName = "buyerId")]
        public string BuyerId { get; set; }

        [JsonProperty(PropertyName = "cartId")]
        public string CartId { get; set; }

        [JsonProperty(PropertyName = "bookedServices")]
        public List<CartItem> BookedServices { get; set; } = new List<CartItem>();

        [JsonConverter(typeof(StringEnumConverter))]
        [JsonProperty(PropertyName = "orderStatus")]
        public CartStatus OrderStatus { get; set; }        

        [JsonProperty(PropertyName = "total")]
        public decimal Total { get; set; }

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
    }

  


}

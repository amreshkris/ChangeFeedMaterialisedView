using System;
using Newtonsoft.Json;

namespace ChangeFeedMaterialisedView
{
    public class Buyer
    {
        [JsonProperty(PropertyName = "buyerId")]
        public string BuyerId { get; set; }

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "name")]
        public string Name{ get; set; } 

        [JsonProperty(PropertyName = "emailId")]
        public string EmailId{ get; set; }

        [JsonProperty(PropertyName = "contactNumber")]
        public string ContactNumber { get; set; }
    }
}

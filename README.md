# Mongo Client (Lua)

A straightforward Lua client for MongoDB Atlas data api (deprecated) check out network-layer branch.

---

## Installation

Copy the module into your project and require it or use wally to install it as package:

```lua
local HttpService = game:GetService("HttpService")
local Mongo = require(path.to.mongo_client)

local client = Mongo.new({
  ApiKey = "YOUR_API_KEY",
  AppId = "app-123",
  DataSource = "Cluster0",

  Debug = false,
  Timeout = 30,

  EnableRateLimiter = true,
  MaxRequestsPerMinute = 60,
  MaxConcurrent = 5,

  EnableRetry = true,
  MaxRetries = 5,
})

-- Finding document
local result = client:FindOne("users", { username = "Zvox" })
if result.success then
  print(result.data and result.data.username)
else
  warn(result.code, result.error)
end

-- Inserting document
local result = client:InsertOne("users", { username = "Zvox" })
if result.success then
  print("Inserted ID:", result.data)
end

-- Updating document
local filter = { username = "Zvox" }
local update = Mongo.Set({ title = "Developer" })

local result = client:UpdateOne("users", filter, update, true) -- upsert enabled

-- Finding multiple documents
local result = client:Find("users", { active = true }, {
  limit = 10,
  sort = { createdAt = -1 },
})

for _, doc in ipairs(result.data or {}) do
  print(doc.email)
end
```
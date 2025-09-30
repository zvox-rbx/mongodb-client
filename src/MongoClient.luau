--!strict
--[[
    MongoDB Atlas Data API Client for Roblox
    Version: 1.0.0
    
    MongoDB Atlas Data API client that provides rate limiting, retry logic, 
    type safety, and request timeout handling.
]]

local HttpService = game:GetService("HttpService")
local ERROR_CODES = {
	VALIDATION = "VALIDATION_ERROR",
	ENCODING = "ENCODING_ERROR",
	DECODING = "DECODING_ERROR",
	HTTP = "HTTP_ERROR",
	DNS = "DNS_ERROR",
	AUTH = "AUTH_ERROR",
	SAFETY = "SAFETY_ERROR",
	NOTFOUND = "NOT_FOUND",
	RATELIMIT = "RATE_LIMIT",
	MAXRETRIES = "MAX_RETRIES_EXCEEDED",
	DEPRECATED = "DEPRECATED_METHOD_PROHIBITED",
	TIMEOUT = "REQUEST_TIMEOUT",
}
local MAX_RECOMMENDED_BATCH_SIZE = 100
local MAX_BACKOFF_MULTIPLIER = 16
local MAX_BACKOFF_MS = 10000
local BASE_BACKOFF_MS = 1000
local DEFAULT_TIMEOUT = 30

export type MongoConfig = {
	ApiKey: string,
	AppId: string,
	DataSource: string?,
	BaseUrl: string?,
	Debug: boolean?,
	Timeout: number?,
	EnableRateLimiter: boolean?,
	MaxRequestsPerMinute: number?,
	MaxConcurrent: number?,
	EnableRetry: boolean?,
	MaxRetries: number?,
	RateLimitPollInterval: number?,
	AllowDeprecatedMethods: boolean?,
	OnApiKeyExpired: ((client: MongoClient) -> ())?,
}

export type Filter = { [string]: any }
export type Document = { [string]: any }
export type Update = { [string]: any }
export type PipelineStage = { [string]: any }
export type Pipeline = { PipelineStage }
export type Sort = { [string]: number }
export type Projection = { [string]: number }

export type FindOptions = {
	limit: number?,
	skip: number?,
	sort: Sort?,
	projection: Projection?,
}

export type PaginationCursor = {
	lastId: { ["$oid"]: string }?,
	skip: number,
	hasMore: boolean,
}

export type PaginatedResult = {
	documents: { Document },
	cursor: PaginationCursor,
}

export type UpdateResult = {
	matchedCount: number,
	modifiedCount: number,
	upsertedId: string?,
}

export type BulkWriteOperation = {
	insertOne: { document: Document }?,
	updateOne: { filter: Filter, update: Update, upsert: boolean? }?,
	updateMany: { filter: Filter, update: Update, upsert: boolean? }?,
	deleteOne: { filter: Filter }?,
	deleteMany: { filter: Filter }?,
	replaceOne: { filter: Filter, replacement: Document, upsert: boolean? }?,
}

export type BulkWriteResult = {
	insertedCount: number,
	matchedCount: number,
	modifiedCount: number,
	deletedCount: number,
	upsertedCount: number,
	upsertedIds: { [number]: string },
	errors: { [number]: string },
}

export type QueryBuilder = {
	Eq: (value: any) -> Filter,
	Ne: (value: any) -> Filter,
	Gt: (value: any) -> Filter,
	Gte: (value: any) -> Filter,
	Lt: (value: any) -> Filter,
	Lte: (value: any) -> Filter,
	In: (values: { any }) -> Filter,
	Nin: (values: { any }) -> Filter,
	Exists: (exists: boolean) -> Filter,
	Regex: (pattern: string, options: string?) -> Filter,
}

export type OperationResult<T> = {
	success: boolean,
	data: T?,
	error: string?,
	code: string?,
}

export type LogFunction = (level: string, message: string, data: any?) -> ()

export type RateLimiterImpl = typeof(setmetatable(
	{} :: {
		maxRequestsPerMinute: number,
		maxConcurrent: number,
		tokens: number,
		lastRefill: number,
		concurrent: number,
		refillRate: number,
		pollInterval: number,
	},
	{} :: {
		__index: any,
	}
))

export type RateLimiter = {
	maxRequestsPerMinute: number,
	maxConcurrent: number,
	tokens: number,
	lastRefill: number,
	concurrent: number,
	refillRate: number,
	pollInterval: number,
	refillTokens: (self: RateLimiter) -> (),
	tryAcquire: (self: RateLimiter) -> boolean,
	release: (self: RateLimiter) -> (),
	waitForToken: (self: RateLimiter) -> (),
}

export type MongoClient = {
	FindOne: (self: MongoClient, collection: string, filter: Filter?, options: FindOptions?) -> OperationResult<Document?>,
	Find: (self: MongoClient, collection: string, filter: Filter?, options: FindOptions?) -> OperationResult<{ Document }>,
	FindPaginated: (self: MongoClient, collection: string, filter: Filter?, limit: number, cursor: PaginationCursor?, fetchExtra: boolean?) -> OperationResult<PaginatedResult>,
	InsertOne: (self: MongoClient, collection: string, document: Document) -> OperationResult<string>,
	InsertMany: (self: MongoClient, collection: string, documents: { Document }) -> OperationResult<{ string }>,
	UpdateOne: (self: MongoClient, collection: string, filter: Filter, update: Update, upsert: boolean?) -> OperationResult<UpdateResult>,
	UpdateMany: (self: MongoClient, collection: string, filter: Filter, update: Update, upsert: boolean?) -> OperationResult<UpdateResult>,
	DeleteOne: (self: MongoClient, collection: string, filter: Filter) -> OperationResult<number>,
	DeleteMany: (self: MongoClient, collection: string, filter: Filter) -> OperationResult<number>,
	ReplaceOne: (self: MongoClient, collection: string, filter: Filter, replacement: Document, upsert: boolean?) -> OperationResult<UpdateResult>,
	Aggregate: (self: MongoClient, collection: string, pipeline: Pipeline, options: FindOptions?) -> OperationResult<{ Document }>,
	Count: (self: MongoClient, collection: string, filter: Filter?, options: FindOptions?) -> OperationResult<number>,
	Distinct: (self: MongoClient, collection: string, field: string, filter: Filter?) -> OperationResult<{ any }>,
	BulkWrite: (self: MongoClient, collection: string, operations: { BulkWriteOperation }) -> OperationResult<BulkWriteResult>,
	FindById: (self: MongoClient, collection: string, id: string) -> OperationResult<Document?>,
	UpdateById: (self: MongoClient, collection: string, id: string, update: Update) -> OperationResult<UpdateResult>,
	DeleteById: (self: MongoClient, collection: string, id: string) -> OperationResult<number>,
	FindAll: (self: MongoClient, collection: string, limit: number?) -> OperationResult<{ Document }>,
	DeleteAll: (self: MongoClient, collection: string) -> OperationResult<number>,
	Query: QueryBuilder,
	Set: (fields: { [string]: any }) -> Update,
	Inc: (fields: { [string]: number }) -> Update,
	Push: (fields: { [string]: any }) -> Update,
	Pull: (fields: { [string]: any }) -> Update,
	AddToSet: (fields: { [string]: any }) -> Update,
	Unset: (fields: { string }) -> Update,
	ObjectId: (self: MongoClient, id: string?) -> { ["$oid"]: string },
	Timestamp: (milliseconds: number?) -> { ["$date"]: { ["$numberLong"]: string } },
	SetDebug: (self: MongoClient, enabled: boolean) -> (),
	GetStats: (self: MongoClient) -> {
		totalRequests: number,
		totalErrors: number,
		avgLatency: number,
		rateLimitHits: number,
		queuedRequests: number,
		successRate: number,
	},
	ResetStats: (self: MongoClient) -> (),
	Disconnect: (self: MongoClient) -> (),
	_requestDirect: (self: MongoClient, endpoint: string, body: { [string]: any }) -> OperationResult<any>,
	_config: MongoConfig,
	_database: string,
	_baseUrl: string,
	_headers: { [string]: string },
	_debug: boolean,
	_log: LogFunction,
	_stats: {
		totalRequests: number,
		totalErrors: number,
		totalLatency: number,
		rateLimitHits: number,
		queuedRequests: number,
	},
	_rateLimiter: RateLimiter?,
	_oidCounter: number,
	_machineId: string,
}

local MongoClientClass = {}
MongoClientClass.__index = MongoClientClass

--[[
    Validates that a value is a non-empty string
    
    Args:
        value (any): Value to validate
        name (string): Parameter name for error messages
    
    Returns:
        boolean: true if valid
        string?: Error message if invalid
]]
local function validateString(value: any, name: string): (boolean, string?)
	if type(value) ~= "string" or value == "" then
		return false, string.format("%s must be a non-empty string", name)
	end
	return true, nil
end

--[[
    Validates that a value is a table
    
    Args:
        value (any): Value to validate
        name (string): Parameter name for error messages
    
    Returns:
        boolean: true if valid
        string?: Error message if invalid
]]
local function validateTable(value: any, name: string): (boolean, string?)
	if type(value) ~= "table" then
		return false, string.format("%s must be a table", name)
	end
	return true, nil
end

--[[
    Validates that a value is a number
    
    Args:
        value (any): Value to validate
        name (string): Parameter name for error messages
    
    Returns:
        boolean: true if valid
        string?: Error message if invalid
]]
local function validateNumber(value: any, name: string): (boolean, string?)
	if type(value) ~= "number" then
		return false, string.format("%s must be a number", name)
	end
	return true, nil
end

--[[
    Validates that a value is a boolean
    
    Args:
        value (any): Value to validate
        name (string): Parameter name for error messages
    
    Returns:
        boolean: true if valid
        string?: Error message if invalid
]]
local function validateBoolean(value: any, name: string): (boolean, string?)
	if type(value) ~= "boolean" then
		return false, string.format("%s must be a boolean", name)
	end
	return true, nil
end

--[[
    Validates that document keys don't contain MongoDB operators
    
    Args:
        document (Document): Document to validate
    
    Returns:
        boolean: true if valid
        string?: Error message if invalid
]]
local function validateDocumentKeys(document: Document): (boolean, string?)
	for documentKey, _ in pairs(document) do
		if type(documentKey) == "string" and string.sub(documentKey, 1, 1) == "$" then
			return false, "Document contains illegal key starting with '$'"
		end
	end
	return true, nil
end

--[[
    Creates a logging function with optional debug mode
    
    Args:
        debugEnabled (boolean): Whether to enable debug logging
    
    Returns:
        LogFunction: Configured logging function
]]
local function makeLogger(debugEnabled: boolean): LogFunction
	return function(level: string, message: string, data: any?)
		if not debugEnabled and level == "DEBUG" then
			return
		end

		local prefix = string.format("[mongodb-client:%s]", level)
		if data and debugEnabled then
			local success, encoded = pcall(HttpService.JSONEncode, HttpService, data)
			if success then
				print(prefix, message, encoded)
			else
				print(prefix, message, "[Unable to encode data]")
			end
		else
			print(prefix, message)
		end
	end
end

local RateLimiterClass = {}
RateLimiterClass.__index = RateLimiterClass

--[[
    Creates a new rate limiter using token bucket algorithm
    
    Args:
        maxRequestsPerMinute (number): Maximum requests allowed per minute
        maxConcurrent (number): Maximum concurrent requests
        pollInterval (number): Polling interval for token availability
    
    Returns:
        RateLimiter: New rate limiter instance
]]
function RateLimiterClass.new(maxRequestsPerMinute: number, maxConcurrent: number, pollInterval: number): RateLimiter
	local self: RateLimiterImpl = setmetatable({
		maxRequestsPerMinute = maxRequestsPerMinute,
		maxConcurrent = maxConcurrent,
		tokens = maxRequestsPerMinute,
		lastRefill = os.clock(),
		concurrent = 0,
		refillRate = maxRequestsPerMinute / 60,
		pollInterval = pollInterval,
	}, RateLimiterClass)
	return self
end

--[[
    Refills tokens based on elapsed time since last refill
]]
function RateLimiterClass:refillTokens()
	local currentTime = os.clock()
	local elapsed = currentTime - self.lastRefill
	local tokensToAdd = elapsed * self.refillRate

	if tokensToAdd >= 1 then
		self.tokens = math.min(self.maxRequestsPerMinute, self.tokens + tokensToAdd)
		self.lastRefill = currentTime
	end
end

--[[
    Attempts to acquire a token for making a request
    
    Returns:
        boolean: true if token acquired, false otherwise
]]
function RateLimiterClass:tryAcquire(): boolean
	self:refillTokens()

	if self.tokens >= 1 and self.concurrent < self.maxConcurrent then
		self.tokens = self.tokens - 1
		self.concurrent = self.concurrent + 1
		return true
	end
	return false
end

--[[
    Releases a concurrent request slot
]]
function RateLimiterClass:release()
	self.concurrent = math.max(0, self.concurrent - 1)
end

--[[
    Waits until a token becomes available for request
]]
function RateLimiterClass:waitForToken()
	while not self:tryAcquire() do
		task.wait(self.pollInterval)
	end
end

--[[
    Generates or validates a MongoDB ObjectId
    
    Args:
        id (string?): Optional existing ObjectId to validate
    
    Returns:
        table: MongoDB ObjectId structure { ["$oid"]: string }
]]
function MongoClientClass:ObjectId(id: string?): { ["$oid"]: string }
	if id then
		local valid, validationError = validateString(id, "ObjectId")
		if not valid then
			error(validationError, 2)
		end
		if #id ~= 24 then
			error(string.format("ObjectId must be exactly 24 hexadecimal characters (got %d)", #id), 2)
		end
		if not string.match(id, "^[0-9a-fA-F]+$") then
			error("ObjectId must contain only hexadecimal characters", 2)
		end
		return { ["$oid"] = string.lower(id) }
	else
		local timestamp = os.time()
		local timestampHex = string.format("%08x", timestamp)

		if not self._machineId then
			local bytes = {}
			for byteIndex = 1, 5 do
				bytes[byteIndex] = string.format("%02x", math.random(0, 255))
			end
			self._machineId = table.concat(bytes)
		end

		if not self._oidCounter then
			self._oidCounter = math.random(0, 0xFFFFFF)
		end
		self._oidCounter = (self._oidCounter + 1) % 0x1000000
		local counterHex = string.format("%06x", self._oidCounter)

		local objectId = timestampHex .. self._machineId .. counterHex
		return { ["$oid"] = objectId }
	end
end

--[[
    Encodes a table to JSON with error handling
    
    Args:
        tableData (any): Table to encode
    
    Returns:
        boolean: Success status
        string: JSON string or error message
]]
local function safeEncode(tableData: any): (boolean, string)
	local success, result = pcall(function()
		return HttpService:JSONEncode(tableData)
	end)
	return success, result
end

--[[
    Decodes JSON string with error handling
    
    Args:
        jsonString (string): JSON string to decode
    
    Returns:
        boolean: Success status
        any: Decoded data or error message
]]
local function safeDecode(jsonString: string): (boolean, any)
	local success, result = pcall(function()
		return HttpService:JSONDecode(jsonString)
	end)
	return success, result
end

--[[
    Makes a direct HTTP request to MongoDB Atlas Data API
    
    Args:
        endpoint (string): API endpoint name
        body (table): Request body data
    
    Returns:
        OperationResult<any>: Request result with data or error
]]
function MongoClientClass:_requestDirect(endpoint: string, body: { [string]: any }): OperationResult<any>
	self._stats.totalRequests = self._stats.totalRequests + 1
	local startTime = os.clock()

	local requestUrl = self._baseUrl .. "/action/" .. endpoint
	body.dataSource = body.dataSource or self._config.DataSource or "Cluster0"
	body.database = body.database or self._database

	local encodeSuccess, bodyJson = safeEncode(body)
	if not encodeSuccess then
		self._stats.totalErrors = self._stats.totalErrors + 1
		return {
			success = false,
			data = nil,
			error = "Failed to encode request body: " .. tostring(bodyJson),
			code = ERROR_CODES.ENCODING,
		}
	end

	if self._debug then
		self._log("DEBUG", "Request", { endpoint = endpoint, url = requestUrl, body = body })
	end

	local timeout = self._config.Timeout or DEFAULT_TIMEOUT
	local completed = false
	local httpSuccess, response
	
	task.spawn(function()
		httpSuccess, response = pcall(function()
			return HttpService:RequestAsync({
				Url = requestUrl,
				Method = "POST",
				Headers = self._headers,
				Body = bodyJson,
			})
		end)
		completed = true
	end)
	
	local elapsed = 0
	local pollInterval = 0.1
	while not completed and elapsed < timeout do
		task.wait(pollInterval)
		elapsed = elapsed + pollInterval
	end
	
	if not completed then
		self._stats.totalErrors = self._stats.totalErrors + 1
		return {
			success = false,
			data = nil,
			error = string.format("Request timeout after %.1fs", timeout),
			code = ERROR_CODES.TIMEOUT,
		}
	end

	local latency = (os.clock() - startTime) * 1000
	self._stats.totalLatency = self._stats.totalLatency + latency

	if not httpSuccess then
		self._stats.totalErrors = self._stats.totalErrors + 1
		local errorMessage = tostring(response)
		local code = ERROR_CODES.HTTP

		if string.find(errorMessage, "DnsResolve") then
			code = ERROR_CODES.DNS
		elseif string.find(errorMessage, "403") or string.find(errorMessage, "401") then
			code = ERROR_CODES.AUTH
		elseif string.find(errorMessage, "404") then
			code = ERROR_CODES.NOTFOUND
		elseif string.find(errorMessage, "Number of requests exceeded") then
			code = ERROR_CODES.RATELIMIT
		end

		self._log("ERROR", string.format("HTTP request failed (%.2fms): %s", latency, errorMessage), nil)

		return {
			success = false,
			data = nil,
			error = errorMessage,
			code = code,
		}
	end

	local httpResponse = response :: any
	if not httpResponse.Success then
		self._stats.totalErrors = self._stats.totalErrors + 1
		return {
			success = false,
			data = nil,
			error = string.format("HTTP %d: %s", httpResponse.StatusCode, httpResponse.StatusMessage or "Unknown"),
			code = ERROR_CODES.HTTP,
		}
	end

	local decodeSuccess, result = safeDecode(httpResponse.Body)
	if not decodeSuccess then
		self._stats.totalErrors = self._stats.totalErrors + 1
		return {
			success = false,
			data = nil,
			error = "Failed to decode response: " .. tostring(result),
			code = ERROR_CODES.DECODING,
		}
	end

	if self._debug then
		self._log("DEBUG", string.format("Response (%.2fms)", latency), result)
	end

	return {
		success = true,
		data = result,
		error = nil,
		code = nil,
	}
end

--[[
    Determines if an error is retryable based on error message
    
    Args:
        errorMessage (string): Error message to analyze
    
    Returns:
        boolean: true if error should be retried
]]
local function isRetryableError(errorMessage: string): boolean
	return not not (
		string.find(errorMessage, "timeout") or
			string.find(errorMessage, "500") or
			string.find(errorMessage, "502") or
			string.find(errorMessage, "503") or
			string.find(errorMessage, "504") or
			string.find(errorMessage, "429")
	)
end

--[[
    Executes request with retry logic and exponential backoff
    
    Args:
        self (MongoClient): Client instance
        endpoint (string): API endpoint
        body (any): Request body
        maxRetries (number): Maximum retry attempts
    
    Returns:
        OperationResult<any>: Final result after retries
]]
local function requestWithRetry(
	self: MongoClient,
	endpoint: string,
	body: any,
	maxRetries: number
): OperationResult<any>
	local attempt = 0
	local lastError = ""

	while attempt < maxRetries do
		attempt = attempt + 1

		if self._rateLimiter then
			self._rateLimiter:waitForToken()
		end

		local result = self:_requestDirect(endpoint, body)

		if self._rateLimiter then
			self._rateLimiter:release()
		end

		if result.success then
			return result
		end

		lastError = result.error or "Unknown error"

		if result.code == ERROR_CODES.AUTH and self._config.OnApiKeyExpired then
			self._log("WARN", "API key may be expired, triggering callback", nil)
			pcall(self._config.OnApiKeyExpired, self)
		end

		if not isRetryableError(lastError) or attempt >= maxRetries then
			return result
		end

		local backoff = math.min(BASE_BACKOFF_MS * math.min(2 ^ (attempt - 1), MAX_BACKOFF_MULTIPLIER), MAX_BACKOFF_MS)
		local jitter = math.random(0, 500)
		local waitTime = (backoff + jitter) / 1000

		self._log("WARN", string.format(
			"Request failed (attempt %d/%d), retrying in %.2fs: %s",
			attempt,
			maxRetries,
			waitTime,
			lastError
			), nil)

		self._stats.rateLimitHits = self._stats.rateLimitHits + 1
		task.wait(waitTime)
	end

	return {
		success = false,
		data = nil,
		error = lastError,
		code = ERROR_CODES.MAXRETRIES,
	}
end

MongoClientClass.Query = {
	Eq = function(value: any): Filter return value end,
	Ne = function(value: any): Filter return { ["$ne"] = value } end,
	Gt = function(value: any): Filter return { ["$gt"] = value } end,
	Gte = function(value: any): Filter return { ["$gte"] = value } end,
	Lt = function(value: any): Filter return { ["$lt"] = value } end,
	Lte = function(value: any): Filter return { ["$lte"] = value } end,
	In = function(values: { any }): Filter return { ["$in"] = values } end,
	Nin = function(values: { any }): Filter return { ["$nin"] = values } end,
	Exists = function(exists: boolean): Filter return { ["$exists"] = exists } end,
	Regex = function(pattern: string, options: string?): Filter
		if options then return { ["$regex"] = pattern, ["$options"] = options } end
		return { ["$regex"] = pattern }
	end,
}

--[[
    Creates a $set update operator
    
    Args:
        fields (table): Fields to set with their values
    
    Returns:
        Update: MongoDB update document
]]
function MongoClientClass.Set(fields: { [string]: any }): Update
	local valid, validationError = validateTable(fields, "Fields")
	if not valid then
		error(validationError, 2)
	end
	return { ["$set"] = fields }
end

--[[
    Creates a $inc update operator for incrementing numeric fields
    
    Args:
        fields (table): Fields to increment with their increment values
    
    Returns:
        Update: MongoDB update document
]]
function MongoClientClass.Inc(fields: { [string]: number }): Update
	local valid, validationError = validateTable(fields, "Fields")
	if not valid then
		error(validationError, 2)
	end
	return { ["$inc"] = fields }
end

--[[
    Creates a $push update operator for adding elements to arrays
    
    Args:
        fields (table): Fields with values to push to arrays
    
    Returns:
        Update: MongoDB update document
]]
function MongoClientClass.Push(fields: { [string]: any }): Update
	local valid, validationError = validateTable(fields, "Fields")
	if not valid then
		error(validationError, 2)
	end
	return { ["$push"] = fields }
end

--[[
    Creates a $pull update operator for removing elements from arrays
    
    Args:
        fields (table): Fields with conditions for elements to remove
    
    Returns:
        Update: MongoDB update document
]]
function MongoClientClass.Pull(fields: { [string]: any }): Update
	local valid, validationError = validateTable(fields, "Fields")
	if not valid then
		error(validationError, 2)
	end
	return { ["$pull"] = fields }
end

--[[
    Creates an $addToSet update operator for adding unique elements to arrays
    
    Args:
        fields (table): Fields with values to add to sets (no duplicates)
    
    Returns:
        Update: MongoDB update document
]]
function MongoClientClass.AddToSet(fields: { [string]: any }): Update
	local valid, validationError = validateTable(fields, "Fields")
	if not valid then
		error(validationError, 2)
	end
	return { ["$addToSet"] = fields }
end

--[[
    Creates an $unset update operator for removing fields from documents
    
    Args:
        fields (table): Array of field names to remove
    
    Returns:
        Update: MongoDB update document
]]
function MongoClientClass.Unset(fields: { string }): Update
	local valid, validationError = validateTable(fields, "Fields")
	if not valid then
		error(validationError, 2)
	end
	local unsetFields = {}
	for _, field in ipairs(fields) do
		unsetFields[field] = ""
	end
	return { ["$unset"] = unsetFields }
end

--[[
    Creates a MongoDB timestamp from milliseconds
    
    Args:
        milliseconds (number?): Optional milliseconds since epoch
    
    Returns:
        table: MongoDB timestamp structure
]]
function MongoClientClass.Timestamp(milliseconds: number?): { ["$date"]: { ["$numberLong"]: string } }
	local timestamp = milliseconds or (os.time() * 1000)
	local valid, validationError = validateNumber(timestamp, "Timestamp")
	if not valid then
		error(validationError, 2)
	end
	return {
		["$date"] = {
			["$numberLong"] = tostring(math.floor(timestamp))
		}
	}
end

--[[
    Validates and fixes empty filter parameters
    
    Args:
        filter (Filter?): MongoDB filter to validate
    
    Returns:
        any: Validated filter or nil
]]
local function fixEmptyFilter(filter: Filter?): any
	if not filter then
		return nil
	end
	if next(filter) == nil then
		error("Empty filter {} is not allowed. Use nil to match all documents or provide a filter like {_id = {['$exists'] = true}}", 3)
	end
	return filter
end

--[[
    Finds a single document in a collection
    
    Args:
        collection (string): Collection name
        filter (Filter?): Optional query filter
        options (FindOptions?): Optional query options
    
    Returns:
        OperationResult<Document?>: Single document or nil
]]
function MongoClientClass:FindOne(
	collection: string,
	filter: Filter?,
	options: FindOptions?
): OperationResult<Document?>
	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { 
			success = false, 
			data = nil, 
			error = string.format("FindOne validation failed: %s", validationError),
			code = ERROR_CODES.VALIDATION 
		}
	end

	local body: any = { collection = collection }

	local fixedFilter = fixEmptyFilter(filter)
	if fixedFilter then
		body.filter = fixedFilter
	end

	if options and options.projection then
		body.projection = options.projection
	end

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "findOne", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = result.data.document,
		error = nil,
		code = nil,
	}
end

--[[
    Finds multiple documents in a collection
    
    Args:
        collection (string): Collection name
        filter (Filter?): Optional query filter
        options (FindOptions?): Optional query options (limit, skip, sort, projection)
    
    Returns:
        OperationResult<{ Document }>: Array of matching documents
]]
function MongoClientClass:Find(
	collection: string,
	filter: Filter?,
	options: FindOptions?
): OperationResult<{ Document }>
	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	local body: any = { collection = collection }

	local fixedFilter = fixEmptyFilter(filter)
	if fixedFilter then
		body.filter = fixedFilter
	end

	if options then
		valid, validationError = validateTable(options, "Options")
		if not valid then
			return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
		end

		if options.limit then
			valid, validationError = validateNumber(options.limit, "Limit")
			if not valid then
				return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
			end
			body.limit = options.limit
		end
		if options.skip then
			valid, validationError = validateNumber(options.skip, "Skip")
			if not valid then
				return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
			end
			body.skip = options.skip
		end
		if options.projection then
			body.projection = options.projection
		end
		if options.sort then
			body.sort = options.sort
		end
	end

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "find", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = result.data.documents or {},
		error = nil,
		code = nil,
	}
end

--[[
    Finds documents with pagination support
    
    Args:
        collection (string): Collection name
        filter (Filter?): Optional query filter
        limit (number): Maximum documents per page
        cursor (PaginationCursor?): Optional pagination cursor
        fetchExtra (boolean?): Whether to fetch one extra document for hasMore detection
    
    Returns:
        OperationResult<PaginatedResult>: Paginated results with cursor
]]
function MongoClientClass:FindPaginated(
	collection: string,
	filter: Filter?,
	limit: number,
	cursor: PaginationCursor?,
	fetchExtra: boolean?
): OperationResult<PaginatedResult>
	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateNumber(limit, "Limit")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	-- Allow configuration of fetch-extra behavior (default true to reduce additional requests)
	if fetchExtra == nil then
		fetchExtra = true
	end

	-- Handle empty filter for pagination (empty filter means "match all" for pagination)
	local paginationFilter = filter
	if filter and type(filter) == "table" and next(filter) == nil then
		paginationFilter = nil -- Convert empty filter to nil for pagination
	end

	local actualLimit = fetchExtra and (limit + 1) or limit
	local skip = cursor and cursor.skip or 0

	local options: FindOptions = {
		limit = actualLimit,
		skip = skip,
	}

	local result = self:Find(collection, paginationFilter, options)

	if not result.success then
		return result :: any
	end

	local documents = result.data :: { Document }
	local hasMore = fetchExtra and (#documents > limit)

	if hasMore then
		table.remove(documents, #documents)
	end

	local lastId = nil
	if #documents > 0 then
		local lastDoc = documents[#documents]
		if lastDoc._id and type(lastDoc._id) == "table" and lastDoc._id["$oid"] then
			lastId = lastDoc._id  -- Keep full ObjectId structure
		end
	end

	return {
		success = true,
		data = {
			documents = documents,
			cursor = {
				lastId = lastId,
				skip = skip + #documents,
				hasMore = hasMore,
			},
		},
		error = nil,
		code = nil,
	}
end

--[[
    Inserts a single document into a collection
    
    Args:
        collection (string): Collection name
        document (Document): Document to insert
    
    Returns:
        OperationResult<string>: Inserted document ID or error
]]
function MongoClientClass:InsertOne(collection: string, document: Document): OperationResult<string>
	local valid, validationError = validateDocumentKeys(document)
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(document, "Document")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	local body = {
		collection = collection,
		document = document,
	}

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "insertOne", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = result.data.insertedId or "",
		error = nil,
		code = nil,
	}
end

--[[
    Inserts multiple documents into a collection
    
    Args:
        collection (string): Collection name
        documents (table): Array of documents to insert
    
    Returns:
        OperationResult<{ string }>: Array of inserted document IDs or error
]]
function MongoClientClass:InsertMany(
	collection: string,
	documents: { Document }
): OperationResult<{ string }>
	for documentIndex, currentDocument in ipairs(documents) do
		local valid, validationError = validateDocumentKeys(currentDocument)
		if not valid then
			return {
				success = false,
				data = nil,
				error = string.format("Document #%d: %s", documentIndex, validationError),
				code = ERROR_CODES.VALIDATION,
			}
		end
	end

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(documents, "Documents")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	if #documents == 0 then
		return {
			success = false,
			data = nil,
			error = "InsertMany requires at least one document",
			code = ERROR_CODES.VALIDATION,
		}
	end

	if #documents > MAX_RECOMMENDED_BATCH_SIZE then
		self._log("WARN", string.format(
			"Inserting %d documents at once. Consider batching in groups of %d to avoid request size limits.",
			#documents,
			MAX_RECOMMENDED_BATCH_SIZE
			), nil)
	end

	local body = {
		collection = collection,
		documents = documents,
	}

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "insertMany", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = result.data.insertedIds or {},
		error = nil,
		code = nil,
	}
end

--[[
    Updates a single document in a collection
    
    Args:
        collection (string): Collection name
        filter (Filter): Query filter to match document
        update (Update): Update operations to apply
        upsert (boolean?): Optional flag to insert if no match found
    
    Returns:
        OperationResult<UpdateResult>: Update operation result
]]
function MongoClientClass:UpdateOne(
	collection: string,
	filter: Filter,
	update: Update,
	upsert: boolean?
): OperationResult<UpdateResult>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(filter, "Filter")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(update, "Update")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	local body: any = {
		collection = collection,
		filter = filter,
		update = update,
	}

	if upsert ~= nil then
		valid, validationError = validateBoolean(upsert, "Upsert")
		if not valid then
			return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
		end
		body.upsert = upsert
	end

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "updateOne", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = {
			matchedCount = result.data.matchedCount or 0,
			modifiedCount = result.data.modifiedCount or 0,
			upsertedId = result.data.upsertedId,
		},
		error = nil,
		code = nil,
	}
end

--[[
    Updates multiple documents in a collection
    
    Args:
        collection (string): Collection name
        filter (Filter): Query filter to match documents
        update (Update): Update operations to apply
        upsert (boolean?): Optional flag to insert if no match found
    
    Returns:
        OperationResult<UpdateResult>: Update operation result
]]
function MongoClientClass:UpdateMany(
	collection: string,
	filter: Filter,
	update: Update,
	upsert: boolean?
): OperationResult<UpdateResult>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(filter, "Filter")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(update, "Update")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	local body: any = {
		collection = collection,
		filter = filter,
		update = update,
	}

	if upsert ~= nil then
		valid, validationError = validateBoolean(upsert, "Upsert")
		if not valid then
			return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
		end
		body.upsert = upsert
	end

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "updateMany", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = {
			matchedCount = result.data.matchedCount or 0,
			modifiedCount = result.data.modifiedCount or 0,
			upsertedId = result.data.upsertedId,
		},
		error = nil,
		code = nil,
	}
end

--[[
    Replaces a single document in a collection
    
    Args:
        collection (string): Collection name
        filter (Filter): Query filter to match document
        replacement (Document): New document to replace with
        upsert (boolean?): Optional flag to insert if no match found
    
    Returns:
        OperationResult<UpdateResult>: Replace operation result
]]
function MongoClientClass:ReplaceOne(
	collection: string,
	filter: Filter,
	replacement: Document,
	upsert: boolean?
): OperationResult<UpdateResult>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(filter, "Filter")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(replacement, "Replacement")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	for replacementKey, _ in pairs(replacement) do
		if string.sub(replacementKey, 1, 1) == "$" then
			return {
				success = false,
				data = nil,
				error = string.format(
					"Replacement document cannot contain update operators like '%s'. Use UpdateOne instead.",
					replacementKey
				),
				code = ERROR_CODES.VALIDATION,
			}
		end
	end

	local body: any = {
		collection = collection,
		filter = filter,
		replacement = replacement,
	}

	if upsert ~= nil then
		valid, validationError = validateBoolean(upsert, "Upsert")
		if not valid then
			return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
		end
		body.upsert = upsert
	end

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "replaceOne", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = {
			matchedCount = result.data.matchedCount or 0,
			modifiedCount = result.data.modifiedCount or 0,
			upsertedId = result.data.upsertedId,
		},
		error = nil,
		code = nil,
	}
end

--[[
    Deletes a single document from a collection
    
    Args:
        collection (string): Collection name
        filter (Filter): Query filter to match document to delete
    
    Returns:
        OperationResult<number>: Number of documents deleted (0 or 1)
]]
function MongoClientClass:DeleteOne(collection: string, filter: Filter): OperationResult<number>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(filter, "Filter")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	if next(filter) == nil then
		return {
			success = false,
			data = nil,
			error = "DeleteOne with empty filter is not allowed. Use DeleteMany with explicit filter or DeleteAll().",
			code = ERROR_CODES.SAFETY,
		}
	end

	local body = {
		collection = collection,
		filter = filter,
	}

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "deleteOne", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = result.data.deletedCount or 0,
		error = nil,
		code = nil,
	}
end

--[[
    Deletes multiple documents from a collection
    
    Args:
        collection (string): Collection name
        filter (Filter): Query filter to match documents to delete
    
    Returns:
        OperationResult<number>: Number of documents deleted
]]
function MongoClientClass:DeleteMany(collection: string, filter: Filter): OperationResult<number>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(filter, "Filter")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	if next(filter) == nil then
		return {
			success = false,
			data = nil,
			error = "DeleteMany with empty filter is dangerous. Use explicit filter or call DeleteAll() if intentional.",
			code = ERROR_CODES.SAFETY,
		}
	end

	local body = {
		collection = collection,
		filter = filter,
	}

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "deleteMany", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = result.data.deletedCount or 0,
		error = nil,
		code = nil,
	}
end

--[[
    Executes an aggregation pipeline on a collection
    
    Args:
        collection (string): Name of the collection to aggregate
        pipeline (Pipeline): Array of aggregation stage objects
    
    Returns:
        OperationResult<{ Document }>: Array of documents or error details
]]
function MongoClientClass:Aggregate(
	collection: string,
	pipeline: Pipeline
): OperationResult<{ Document }>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(pipeline, "Pipeline")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	if #pipeline == 0 then
		self._log("WARN", "Empty aggregation pipeline - this will return all documents", nil)
	end

	for stageIndex, pipelineStage in ipairs(pipeline) do
		valid, validationError = validateTable(pipelineStage, string.format("Pipeline stage %d", stageIndex))
		if not valid then
			return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
		end

		if pipelineStage["$group"] and not pipelineStage["$group"]._id then
			return {
				success = false,
				data = nil,
				error = string.format(
					"Pipeline stage %d: $group requires an _id field",
					stageIndex
				),
				code = ERROR_CODES.VALIDATION,
			}
		end
	end

	local body = {
		collection = collection,
		pipeline = pipeline,
	}

	local maxRetries = (self._config.EnableRetry and self._config.MaxRetries) or 1
	local result = requestWithRetry(self, "aggregate", body, maxRetries)

	if not result.success then
		return result :: any
	end

	return {
		success = true,
		data = result.data.documents or {},
		error = nil,
		code = nil,
	}
end

--[[
    Counts documents in a collection that match a filter
    
    Args:
        collection (string): Name of the collection to count documents in
        filter (Filter?): Optional filter to match documents (nil counts all)
    
    Returns:
        OperationResult<number>: Count as number or error details
]]
function MongoClientClass:Count(
	collection: string,
	filter: Filter?
): OperationResult<number>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	local pipeline: Pipeline = {}

	if filter and next(filter) ~= nil then
		table.insert(pipeline, { ["$match"] = filter })
	end

	table.insert(pipeline, { ["$count"] = "total" })

	local result = self:Aggregate(collection, pipeline, nil)

	if not result.success then
		return result :: any
	end

	local documents = result.data :: { Document }
	local count = 0
	if documents and #documents > 0 and documents[1].total then
		count = documents[1].total
	end

	return {
		success = true,
		data = count,
		error = nil,
		code = nil,
	}
end

--[[
    Finds distinct values for a field in a collection
    
    Args:
        collection (string): Name of the collection to query
        field (string): Field name to get distinct values for
        filter (Filter?): Optional filter to match documents
    
    Returns:
        OperationResult<{ any }>: Array of distinct values or error details
]]
function MongoClientClass:Distinct(
	collection: string,
	field: string,
	filter: Filter?
): OperationResult<{ any }>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateString(field, "Field")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	local pipeline: Pipeline = {}

	if filter and next(filter) ~= nil then
		table.insert(pipeline, { ["$match"] = filter })
	end

	table.insert(pipeline, { ["$group"] = { _id = "$" .. field } })
	table.insert(pipeline, { ["$project"] = { _id = 0, value = "$_id" } })

	local result = self:Aggregate(collection, pipeline, nil)

	if not result.success then
		return result :: any
	end

	local documents = result.data :: { Document }
	local values = {}
	for _, currentDocument in ipairs(documents) do
		if currentDocument.value ~= nil then
			table.insert(values, currentDocument.value)
		end
	end

	return {
		success = true,
		data = values,
		error = nil,
		code = nil,
	}
end

--[[
    Executes multiple write operations in a single request
    
    Args:
        collection (string): Name of the collection to perform operations on
        operations ({ BulkWriteOperation }): Array of bulk write operations (insert, update, delete)
    
    Returns:
        OperationResult<BulkWriteResult>: BulkWriteResult with operation counts or error details
]]
function MongoClientClass:BulkWrite(
	collection: string,
	operations: { BulkWriteOperation }
): OperationResult<BulkWriteResult>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateTable(operations, "Operations")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	if #operations == 0 then
		return {
			success = false,
			data = nil,
			error = "BulkWrite requires at least one operation",
			code = ERROR_CODES.VALIDATION,
		}
	end

	if #operations > MAX_RECOMMENDED_BATCH_SIZE then
		self._log("WARN", string.format(
			"Performing %d bulk operations. Consider batching in groups of %d to avoid request size limits.",
			#operations,
			MAX_RECOMMENDED_BATCH_SIZE
			), nil)
	end

	local result: BulkWriteResult = {
		insertedCount = 0,
		matchedCount = 0,
		modifiedCount = 0,
		deletedCount = 0,
		upsertedCount = 0,
		upsertedIds = {},
		errors = {},
	}

	for operationIndex, operation in ipairs(operations) do
		local success, opResult = pcall(function()
			if operation.insertOne then
				local insertResult = self:InsertOne(collection, operation.insertOne.document)
				if insertResult.success then
					result.insertedCount = result.insertedCount + 1
					result.upsertedIds[operationIndex] = insertResult.data :: string
				else
					result.errors[operationIndex] = insertResult.error or "Unknown error"
				end

			elseif operation.updateOne then
				local updateResult = self:UpdateOne(
					collection,
					operation.updateOne.filter,
					operation.updateOne.update,
					operation.updateOne.upsert
				)
				if updateResult.success then
					local data = updateResult.data :: UpdateResult
					result.matchedCount = result.matchedCount + data.matchedCount
					result.modifiedCount = result.modifiedCount + data.modifiedCount
					if data.upsertedId then
						result.upsertedCount = result.upsertedCount + 1
						result.upsertedIds[operationIndex] = data.upsertedId
					end
				else
					result.errors[operationIndex] = updateResult.error or "Unknown error"
				end

			elseif operation.updateMany then
				local updateResult = self:UpdateMany(
					collection,
					operation.updateMany.filter,
					operation.updateMany.update,
					operation.updateMany.upsert
				)
				if updateResult.success then
					local data = updateResult.data :: UpdateResult
					result.matchedCount = result.matchedCount + data.matchedCount
					result.modifiedCount = result.modifiedCount + data.modifiedCount
					if data.upsertedId then
						result.upsertedCount = result.upsertedCount + 1
						result.upsertedIds[operationIndex] = data.upsertedId
					end
				else
					result.errors[operationIndex] = updateResult.error or "Unknown error"
				end

			elseif operation.deleteOne then
				local deleteResult = self:DeleteOne(collection, operation.deleteOne.filter)
				if deleteResult.success then
					result.deletedCount = result.deletedCount + (deleteResult.data :: number)
				else
					result.errors[operationIndex] = deleteResult.error or "Unknown error"
				end

			elseif operation.deleteMany then
				local deleteResult = self:DeleteMany(collection, operation.deleteMany.filter)
				if deleteResult.success then
					result.deletedCount = result.deletedCount + (deleteResult.data :: number)
				else
					result.errors[operationIndex] = deleteResult.error or "Unknown error"
				end

			elseif operation.replaceOne then
				local replaceResult = self:ReplaceOne(
					collection,
					operation.replaceOne.filter,
					operation.replaceOne.replacement,
					operation.replaceOne.upsert
				)
				if replaceResult.success then
					local data = replaceResult.data :: UpdateResult
					result.matchedCount = result.matchedCount + data.matchedCount
					result.modifiedCount = result.modifiedCount + data.modifiedCount
					if data.upsertedId then
						result.upsertedCount = result.upsertedCount + 1
						result.upsertedIds[operationIndex] = data.upsertedId
					end
				else
					result.errors[operationIndex] = replaceResult.error or "Unknown error"
				end

			else
				self._log("WARN", string.format("Unknown bulk operation at index %d", operationIndex), nil)
				result.errors[operationIndex] = "Unknown operation type"
			end
		end)

		if not success then
			result.errors[operationIndex] = tostring(opResult)
			self._log("ERROR", string.format("Bulk operation %d failed: %s", operationIndex, tostring(opResult)), nil)
		end
	end

	return {
		success = true,
		data = result,
		error = nil,
		code = nil,
	}
end

--[[
    Enables or disables debug logging
    
    Args:
        enabled (boolean): Whether to enable debug mode
]]
function MongoClientClass:SetDebug(enabled: boolean)

	local valid, validationError = validateBoolean(enabled, "Enabled")
	if not valid then
		error(validationError, 2)
	end
	self._debug = enabled
	self._log = makeLogger(enabled)
	self._log("INFO", string.format("Debug mode %s", enabled and "enabled" or "disabled"), nil)
end

--[[
    Gets client statistics including request counts, latency, and success rate
    
    Returns:
        Statistics object with request counts, latency, and success rate
]]
function MongoClientClass:GetStats(): {
	totalRequests: number,
	totalErrors: number,
	avgLatency: number,
	rateLimitHits: number,
	queuedRequests: number,
	successRate: number,
	}

	local successRate = 0
	if self._stats.totalRequests > 0 then
		local successfulRequests = self._stats.totalRequests - self._stats.totalErrors
		successRate = (successfulRequests / self._stats.totalRequests) * 100
	end

	return {
		totalRequests = self._stats.totalRequests,
		totalErrors = self._stats.totalErrors,
		avgLatency = (self._stats.totalRequests > 0)
			and (self._stats.totalLatency / self._stats.totalRequests)
			or 0,
		rateLimitHits = self._stats.rateLimitHits,
		queuedRequests = self._stats.queuedRequests,
		successRate = successRate,
	}
end

--[[
    Resets all client statistics to zero
]]
function MongoClientClass:ResetStats()

	self._stats = {
		totalRequests = 0,
		totalErrors = 0,
		totalLatency = 0,
		rateLimitHits = 0,
		queuedRequests = 0,
	}
	self._log("INFO", "Statistics reset", nil)
end

--[[
    Disconnects from MongoDB and clears all configuration
]]
function MongoClientClass:Disconnect()

	self._log("INFO", "Disconnecting from MongoDB", nil)
	self._config = nil :: any
	self._database = ""
	self._headers = {}
	if self._rateLimiter then
		self._rateLimiter = nil
	end
end

--[[
    Finds a document by its ObjectId
    
    Args:
        collection (string): Collection name
        id (string): ObjectId string
    
    Returns:
        OperationResult<Document?>: Document or nil
]]
function MongoClientClass:FindById(collection: string, id: string): OperationResult<Document?>

	return self:FindOne(collection, { _id = self:ObjectId(id) })
end

--[[
    Updates a document by its ObjectId
    
    Args:
        collection (string): Collection name
        id (string): ObjectId string
        update (Update): Update operations to apply
    
    Returns:
        OperationResult<UpdateResult>: Update operation result
]]
function MongoClientClass:UpdateById(
	collection: string,
	id: string,
	update: Update
): OperationResult<UpdateResult>

	return self:UpdateOne(collection, { _id = self:ObjectId(id) }, update, false)
end

--[[
    Deletes a document by its ObjectId
    
    Args:
        collection (string): Collection name
        id (string): ObjectId string
    
    Returns:
        OperationResult<number>: Number of documents deleted (0 or 1)
]]
function MongoClientClass:DeleteById(collection: string, id: string): OperationResult<number>

	local valid, validationError = validateString(collection, "Collection")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	valid, validationError = validateString(id, "ID")
	if not valid then
		return { success = false, data = nil, error = validationError, code = ERROR_CODES.VALIDATION }
	end

	return self:DeleteOne(collection, { _id = self:ObjectId(id) })
end

--[[
    Finds all documents in a collection (deprecated)
    
    Args:
        collection (string): Collection name
        limit (number?): Optional limit
    
    Returns:
        OperationResult<{ Document }>: All documents or error
]]
function MongoClientClass:FindAll(collection: string, limit: number?): OperationResult<{ Document }>


	if not self._config.AllowDeprecatedMethods then
		return {
			success = false,
			data = nil,
			error = "FindAll is deprecated and disabled. Set AllowDeprecatedMethods=true in config to enable, or use Find(collection, nil, {limit=n}).",
			code = ERROR_CODES.DEPRECATED,
		}
	end

	warn("[mongodb-client] FindAll is deprecated. Use Find(collection, nil, {limit=n}) instead.")

	local options: FindOptions? = nil
	if limit then
		options = { limit = limit }
	else
		warn("[mongodb-client] FindAll without limit can cause performance issues on large collections!")
	end

	return self:Find(collection, nil, options)
end

--[[
    Deletes all documents in a collection (deprecated)
    
    Args:
        collection (string): Collection name
    
    Returns:
        OperationResult<number>: Number of documents deleted
]]
function MongoClientClass:DeleteAll(collection: string): OperationResult<number>


	if not self._config.AllowDeprecatedMethods then
		return {
			success = false,
			data = nil,
			error = "DeleteAll is deprecated and disabled. Set AllowDeprecatedMethods=true in config to enable.",
			code = ERROR_CODES.DEPRECATED,
		}
	end

	warn("[mongodb-client] DeleteAll is a dangerous operation. Proceeding to delete ALL documents in collection: " .. collection)

	return self:DeleteMany(collection, { _id = { ["$exists"] = true } })
end

local MongoDB = {}

--[[
    Creates a new MongoDB client connection
    
    Args:
        database (string): Database name to connect to
        config (MongoConfig): Client configuration options
    
    Returns:
        MongoClient: Configured MongoDB client instance
]]
function MongoDB.Connect(database: string, config: MongoConfig): MongoClient
	local valid, validationError = validateString(database, "Database")
	if not valid then
		error(validationError, 2)
	end

	valid, validationError = validateTable(config, "Config")
	if not valid then
		error(validationError, 2)
	end

	valid, validationError = validateString(config.ApiKey, "ApiKey")
	if not valid then
		error(validationError, 2)
	end

	valid, validationError = validateString(config.AppId, "AppId")
	if not valid then
		error(validationError, 2)
	end

	local self = setmetatable({}, MongoClientClass) :: any

	self._config = config
	self._database = database
	self._debug = config.Debug or false
	self._log = makeLogger(self._debug)

	self._stats = {
		totalRequests = 0,
		totalErrors = 0,
		totalLatency = 0,
		rateLimitHits = 0,
		queuedRequests = 0,
	}

	-- ObjectId state per client instance
	self._oidCounter = math.random(0, 0xFFFFFF)
	self._machineId = nil  -- Generated on first ObjectId call

	if config.EnableRateLimiter then
		local maxRequestsPerMinute = config.MaxRequestsPerMinute or 450
		local maxConcurrent = config.MaxConcurrent or 10
		local pollInterval = config.RateLimitPollInterval or 0.05
		self._rateLimiter = RateLimiterClass.new(maxRequestsPerMinute, maxConcurrent, pollInterval)
		self._log("INFO", string.format(
			"Rate limiter enabled: %d req/min, %d concurrent, %.3fs poll",
			maxRequestsPerMinute,
			maxConcurrent,
			pollInterval
			), nil)
	end

	local dataSource = config.DataSource or "Cluster0"
	if config.BaseUrl then
		self._baseUrl = config.BaseUrl
	else
		self._baseUrl = string.format(
			"https://data.mongodb-api.com/app/%s/endpoint/data/v1",
			config.AppId
		)
	end

	self._headers = {
		["Content-Type"] = "application/json",
		["Accept"] = "application/json",
		["api-key"] = config.ApiKey,
	}

	self._log("INFO", string.format("Connected to database '%s' (DataSource: %s)", database, dataSource), nil)
	self._log("INFO", string.format("Base URL: %s", self._baseUrl), nil)

	task.spawn(function()
		local testResult = self:Count("_connection_test", {})
		if not testResult.success then
			self._log("WARN", "Connection test failed - verify your configuration", nil)
			if self._debug then
				self._log("DEBUG", "Connection error: " .. (testResult.error or "Unknown"), nil)
			end
		else
			self._log("INFO", "Connection test completed", nil)
		end
	end)

	return self :: MongoClient
end

return MongoDB

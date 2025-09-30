import { MongoClient, Db, Collection, ObjectId } from 'mongodb';
import type { Document, InsertOneResult, InsertManyResult, UpdateResult, DeleteResult } from 'mongodb';

type LogLevel = 'ERROR' | 'WARN' | 'INFO' | 'DEBUG';

interface NetworkConfig {
    connectionString: string;
    databaseName: string;
    port: number;
    connectTimeoutMS?: number;
    socketTimeoutMS?: number;
    maxPoolSize?: number;
    enableDebug?: boolean;
}

interface DataApiFilter {
    [key: string]: any;
}

interface DataApiDocument {
    [key: string]: any;
}

interface DataApiUpdate {
    [key: string]: any;
}

interface DataApiPipeline {
    [key: string]: any;
}

interface DataApiRequest {
    dataSource: string;
    database: string;
    collection: string;
    filter?: DataApiFilter;
    document?: DataApiDocument;
    documents?: DataApiDocument[];
    update?: DataApiUpdate;
    replacement?: DataApiDocument;
    pipeline?: DataApiPipeline[];
    limit?: number;
    skip?: number;
    sort?: { [key: string]: 1 | -1 };
    projection?: { [key: string]: 0 | 1 };
    upsert?: boolean;
}

interface DataApiResponse<T = any> {
    document?: T;
    documents?: T[];
    insertedId?: string;
    insertedIds?: string[];
    matchedCount?: number;
    modifiedCount?: number;
    deletedCount?: number;
    upsertedId?: string;
}

interface ValidationError {
    field: string;
    message: string;
    code: string;
}

class NetworkLayer {
    private client: MongoClient;
    private db: Db | null = null;
    private isConnected = false;
    private config: NetworkConfig;
    private server: any;

    constructor(config: NetworkConfig) {
        this.config = config;
        this.client = new MongoClient(config.connectionString, {
            connectTimeoutMS: config.connectTimeoutMS || 30000,
            socketTimeoutMS: config.socketTimeoutMS || 30000,
            maxPoolSize: config.maxPoolSize || 10,
            retryWrites: true,
            retryReads: true,
        });
    }

    private log(level: LogLevel, message: string, data?: any): void {
        const timestamp = new Date().toISOString();
        const prefix = `[${timestamp}] [${level}] [mongodb-network-layer]`;
        
        if (data && this.config.enableDebug) {
            console.log(`${prefix} ${message}`, JSON.stringify(data, null, 2));
        } else {
            console.log(`${prefix} ${message}`);
        }
    }

    async connect(): Promise<void> {
        if (this.isConnected) return;

        try {
            await this.client.connect();
            this.db = this.client.db(this.config.databaseName);
            this.isConnected = true;
            this.log('INFO', `Connected to MongoDB database: ${this.config.databaseName}`);
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : 'Unknown connection error';
            this.log('ERROR', `Failed to connect to MongoDB: ${errorMessage}`);
            throw error;
        }
    }

    async disconnect(): Promise<void> {
        if (!this.isConnected) return;

        try {
            await this.client.close();
            this.db = null;
            this.isConnected = false;
            this.log('INFO', 'Disconnected from MongoDB');
        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : 'Unknown disconnection error';
            this.log('ERROR', `Error during MongoDB disconnect: ${errorMessage}`);
            throw error;
        }
    }

    private getCollection(name: string): Collection {
        if (!this.db) {
            throw new Error('Database connection not established');
        }
        return this.db.collection(name);
    }

    private convertObjectIds(obj: any): any {
        if (obj === null || obj === undefined) return obj;
        
        if (Array.isArray(obj)) {
            return obj.map(item => this.convertObjectIds(item));
        }
        
        if (typeof obj === 'object') {
            const result: any = {};
            for (const [key, value] of Object.entries(obj)) {
                if (key === '$oid' && typeof value === 'string') {
                    try {
                        return new ObjectId(value);
                    } catch (error) {
                        throw new Error(`Invalid ObjectId format: ${value}`);
                    }
                } else if (typeof value === 'object' && value !== null && '$oid' in value && typeof (value as any).$oid === 'string') {
                    try {
                        result[key] = new ObjectId((value as any).$oid);
                    } catch (error) {
                        throw new Error(`Invalid ObjectId format in field '${key}': ${(value as any).$oid}`);
                    }
                } else if (key === '_id' && typeof value === 'string' && value.length === 24 && /^[0-9a-fA-F]{24}$/.test(value)) {
                    try {
                        result[key] = new ObjectId(value);
                    } catch (error) {
                        result[key] = value;
                    }
                } else {
                    result[key] = this.convertObjectIds(value);
                }
            }
            return result;
        }
        
        return obj;
    }

    /**
     * Converts native ObjectId instances to MongoDB extended JSON format
     */
    private serializeObjectIds(obj: any): any {
        if (obj === null || obj === undefined) return obj;
        
        if (Array.isArray(obj)) {
            return obj.map(item => this.serializeObjectIds(item));
        }
        
        if (obj instanceof ObjectId) {
            return { $oid: obj.toHexString() };
        }
        
        if (typeof obj === 'object') {
            const result: any = {};
            for (const [key, value] of Object.entries(obj)) {
                result[key] = this.serializeObjectIds(value);
            }
            return result;
        }
        
        return obj;
    }

    /**
     * Validates request parameters for MongoDB operations
     */
    private validateRequest(action: string, body: any): ValidationError[] {
        const errors: ValidationError[] = [];
        const requiredFields: { [action: string]: string[] } = {
            findOne: ['collection'],
            find: ['collection'],
            insertOne: ['collection', 'document'],
            insertMany: ['collection', 'documents'],
            updateOne: ['collection', 'filter', 'update'],
            updateMany: ['collection', 'filter', 'update'],
            replaceOne: ['collection', 'filter', 'replacement'],
            deleteOne: ['collection', 'filter'],
            deleteMany: ['collection', 'filter'],
            aggregate: ['collection', 'pipeline'],
        };
        
        if (!body || typeof body !== 'object') {
            errors.push({
                field: 'body',
                message: 'Request body must be valid JSON object',
                code: 'INVALID_BODY'
            });
            return errors;
        }

        const required = requiredFields[action];
        if (!required) {
            errors.push({
                field: 'action',
                message: `Unsupported operation: ${action}`,
                code: 'UNKNOWN_ACTION'
            });
            return errors;
        }

        for (const field of required) {
            if (!(field in body) || body[field] === null || body[field] === undefined) {
                errors.push({
                    field,
                    message: `Missing required parameter: ${field}`,
                    code: 'MISSING_FIELD'
                });
            }
        }

        if (body.collection && typeof body.collection !== 'string') {
            errors.push({
                field: 'collection',
                message: 'Collection name must be string',
                code: 'INVALID_TYPE'
            });
        }

        if (body.documents && (!Array.isArray(body.documents) || body.documents.length === 0)) {
            errors.push({
                field: 'documents',
                message: 'Documents must be non-empty array',
                code: 'INVALID_TYPE'
            });
        }

        if ((action === 'deleteOne' || action === 'deleteMany') && body.filter && Object.keys(body.filter).length === 0) {
            errors.push({
                field: 'filter',
                message: 'Empty filter not permitted for delete operations',
                code: 'UNSAFE_OPERATION'
            });
        }

        return errors;
    }

    /**
     * Handles MongoDB operation errors and returns appropriate HTTP responses
     */
    private handleMongoError(error: any): Response {
        this.log('ERROR', 'MongoDB operation failed', {
            name: error.name,
            message: error.message,
            code: error.code
        });
        
        if (error.code === 11000) {
            return new Response(JSON.stringify({
                error: 'E11000 duplicate key error collection',
                code: 'DUPLICATE_KEY'
            }), {
                status: 500,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        if (error.code === 2) {
            return new Response(JSON.stringify({
                error: 'Invalid query syntax',
                code: 'INVALID_QUERY'
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        if (error.message && error.message.includes('Invalid ObjectId')) {
            return new Response(JSON.stringify({
                error: error.message,
                code: 'INVALID_OBJECTID'
            }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        if (error.name === 'MongoNetworkError' || error.name === 'MongoNetworkTimeoutError') {
            return new Response(JSON.stringify({
                error: 'Database connection failed',
                code: 'CONNECTION_ERROR'
            }), {
                status: 503,
                headers: { 'Content-Type': 'application/json' }
            });
        }
        
        return new Response(JSON.stringify({
            error: error.message || 'Internal server error',
            code: 'INTERNAL_ERROR'
        }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }

    /**
     * Creates validation error response
     */
    private createValidationResponse(errors: ValidationError[]): Response {
        const message = errors.length === 1 ? 
            errors[0]!.message : 
            `Multiple validation errors: ${errors.map(e => `${e.field}: ${e.message}`).join('; ')}`;
        
        return new Response(JSON.stringify({
            error: message,
            details: errors,
            code: 'VALIDATION_ERROR'
        }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
        });
    }

    /**
     * Executes MongoDB findOne operation
     */
    async findOne(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const filter = this.convertObjectIds(request.filter || {});
        const options: any = {};
        
        if (request.projection) options.projection = request.projection;
        
        const document = await collection.findOne(filter, options);
        
        return {
            document: document ? this.serializeObjectIds(document) : null
        };
    }

    /**
     * Executes MongoDB find operation
     */
    async find(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const filter = this.convertObjectIds(request.filter || {});
        const options: any = {};
        
        if (request.limit) options.limit = request.limit;
        if (request.skip) options.skip = request.skip;
        if (request.sort) options.sort = request.sort;
        if (request.projection) options.projection = request.projection;

        const cursor = collection.find(filter, options);
        const documents = await cursor.toArray();
        
        return {
            documents: documents.map((doc: Document) => this.serializeObjectIds(doc))
        };
    }

    /**
     * Executes MongoDB insertOne operation
     */
    async insertOne(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const document = this.convertObjectIds(request.document!);
        
        const result: InsertOneResult = await collection.insertOne(document);
        
        return {
            insertedId: result.insertedId.toHexString()
        };
    }

    /**
     * Executes MongoDB insertMany operation
     */
    async insertMany(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const documents = request.documents!.map(doc => this.convertObjectIds(doc));
        
        const result: InsertManyResult = await collection.insertMany(documents);
        
        return {
            insertedIds: Object.values(result.insertedIds).map((id: ObjectId) => id.toHexString())
        };
    }

    /**
     * Executes MongoDB updateOne operation
     */
    async updateOne(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const filter = this.convertObjectIds(request.filter!);
        const update = this.convertObjectIds(request.update!);
        
        const result: UpdateResult = await collection.updateOne(filter, update, {
            upsert: request.upsert || false
        });
        
        return {
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount,
            upsertedId: result.upsertedId?.toHexString()
        };
    }

    /**
     * Executes MongoDB updateMany operation
     */
    async updateMany(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const filter = this.convertObjectIds(request.filter!);
        const update = this.convertObjectIds(request.update!);
        
        const result: UpdateResult = await collection.updateMany(filter, update, {
            upsert: request.upsert || false
        });
        
        return {
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount,
            upsertedId: result.upsertedId?.toHexString()
        };
    }

    /**
     * Executes MongoDB replaceOne operation
     */
    async replaceOne(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const filter = this.convertObjectIds(request.filter!);
        const replacement = this.convertObjectIds(request.replacement!);
        
        const result: UpdateResult = await collection.replaceOne(filter, replacement, {
            upsert: request.upsert || false
        });
        
        return {
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount,
            upsertedId: result.upsertedId?.toHexString()
        };
    }

    /**
     * Executes MongoDB deleteOne operation
     */
    async deleteOne(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const filter = this.convertObjectIds(request.filter!);
        
        const result = await collection.deleteOne(filter);
        
        return {
            deletedCount: result.deletedCount
        };
    }

    /**
     * Executes MongoDB deleteMany operation
     */
    async deleteMany(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const filter = this.convertObjectIds(request.filter!);
        
        const result = await collection.deleteMany(filter);
        
        return {
            deletedCount: result.deletedCount
        };
    }

    /**
     * Executes MongoDB aggregate operation
     */
    async aggregate(request: DataApiRequest): Promise<DataApiResponse> {
        const collection = this.getCollection(request.collection);
        const pipeline = this.convertObjectIds(request.pipeline!);
        
        const cursor = collection.aggregate(pipeline);
        const documents = await cursor.toArray();
        
        return {
            documents: documents.map((doc: Document) => this.serializeObjectIds(doc))
        };
    }

    /**
     * Handles MongoDB Data API request processing
     */
    private async handleRequest(action: string, request: Request): Promise<Response> {
        try {
            const body = await request.json() as DataApiRequest;
            
            const validationErrors = this.validateRequest(action, body);
            if (validationErrors.length > 0) {
                return this.createValidationResponse(validationErrors);
            }

            let result: DataApiResponse;
            
            switch (action) {
                case 'findOne': result = await this.findOne(body); break;
                case 'find': result = await this.find(body); break;
                case 'insertOne': result = await this.insertOne(body); break;
                case 'insertMany': result = await this.insertMany(body); break;
                case 'updateOne': result = await this.updateOne(body); break;
                case 'updateMany': result = await this.updateMany(body); break;
                case 'replaceOne': result = await this.replaceOne(body); break;
                case 'deleteOne': result = await this.deleteOne(body); break;
                case 'deleteMany': result = await this.deleteMany(body); break;
                case 'aggregate': result = await this.aggregate(body); break;
                default:
                    return new Response(JSON.stringify({
                        error: `Unsupported operation: ${action}`
                    }), {
                        status: 400,
                        headers: { 'Content-Type': 'application/json' }
                    });
            }
            
            return new Response(JSON.stringify(result), {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
            });
            
        } catch (error) {
            return this.handleMongoError(error);
        }
    }

    /**
     * Returns database health status
     */
    private async healthCheck(): Promise<Response> {
        const connected = this.isConnected;
        let pingable = false;
        
        try {
            if (this.db) {
                await this.db.admin().ping();
                pingable = true;
            }
        } catch (error) {
            // Ping failed
        }
        
        const status = connected && pingable ? 200 : 503;
        
        return new Response(JSON.stringify({
            status: connected && pingable ? 'healthy' : 'unhealthy',
            database: this.config.databaseName,
            connected,
            pingable,
            timestamp: new Date().toISOString()
        }), {
            status,
            headers: { 'Content-Type': 'application/json' }
        });
    }

    /**
     * Adds CORS headers to response
     */
    private addCorsHeaders(response: Response): Response {
        response.headers.set('Access-Control-Allow-Origin', '*');
        response.headers.set('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
        response.headers.set('Access-Control-Allow-Headers', 'Content-Type, Authorization, api-key');
        return response;
    }

    /**
     * Starts HTTP server for MongoDB Data API endpoints
     */
    async start(): Promise<void> {
        this.server = Bun.serve({
            port: this.config.port,
            fetch: async (request) => {
                const url = new URL(request.url);
                
                if (request.method === 'OPTIONS') {
                    return this.addCorsHeaders(new Response(null, { status: 204 }));
                }
                
                if (url.pathname === '/health' && request.method === 'GET') {
                    return this.addCorsHeaders(await this.healthCheck());
                }
                
                const actionMatch = url.pathname.match(/^\/app\/[^/]+\/endpoint\/data\/v1\/action\/(.+)$/);
                if (actionMatch && actionMatch[1] && request.method === 'POST') {
                    const action = actionMatch[1];
                    return this.addCorsHeaders(await this.handleRequest(action, request));
                }
                
                const simpleActionMatch = url.pathname.match(/^\/action\/(.+)$/);
                if (simpleActionMatch && simpleActionMatch[1] && request.method === 'POST') {
                    const action = simpleActionMatch[1];
                    return this.addCorsHeaders(await this.handleRequest(action, request));
                }
                
                return this.addCorsHeaders(new Response(JSON.stringify({
                    error: 'Not found',
                    endpoints: [
                        'GET /health',
                        'POST /action/{operation}',
                        'POST /app/{appId}/endpoint/data/v1/action/{operation}'
                    ]
                }), {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' }
                }));
            }
        });

        this.log('INFO', `MongoDB Data API server started on port ${this.config.port}`);
        this.log('INFO', `Database: ${this.config.databaseName}`);
        this.log('INFO', `Health endpoint: http://localhost:${this.config.port}/health`);
    }

    /**
     * Stops HTTP server and closes database connection
     */
    async stop(): Promise<void> {
        if (this.server) {
            this.server.stop();
            this.log('INFO', 'HTTP server stopped');
        }
        
        await this.disconnect();
    }
}

/**
 * Main application entry point
 */
async function main(): Promise<void> {
    const config: NetworkConfig = {
        connectionString: process.env.MONGO_URI || 'mongodb://localhost:27017',
        databaseName: process.env.DB_NAME || 'test',
        port: parseInt(process.env.PORT || '3000'),
        connectTimeoutMS: 30000,
        socketTimeoutMS: 30000,
        maxPoolSize: 10,
        enableDebug: process.env.NODE_ENV === 'development'
    };

    const networkLayer = new NetworkLayer(config);

    try {
        await networkLayer.connect();
    } catch (error) {
        console.warn('[WARN] Failed to connect to MongoDB at startup. Service will start anyway.');
        console.warn('[WARN] MongoDB operations will fail until connection is established.');
    }

    await networkLayer.start();

    const gracefulShutdown = async () => {
        console.log('[INFO] Shutting down gracefully...');
        await networkLayer.stop();
        process.exit(0);
    };

    process.on('SIGINT', gracefulShutdown);
    process.on('SIGTERM', gracefulShutdown);
}

if (import.meta.main) {
    main().catch(error => {
        console.error('[ERROR] Application startup failed:', error);
        process.exit(1);
    });
}
const {MongoClient} = require("mongodb");
const assert = require("assert");
const {v4: uuidv4} = require('uuid');

// Connection URI
// run mongo with command `docker run  -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=pass -p 27017:27017 mongo:latest`
const uri = "mongodb://admin:pass@localhost";
// Create a new MongoClient
const client = new MongoClient(uri);

const DEFAULT_PAYMENT_ID = "payment123456";

const FINAL_STATUSES = ["SUCCEED", "FAILED"]
// NEW -> WAITING_FOR_SENDING -> SENT_TO_POOL -> SUCCEED | FAILED
async function run() {
    try {
        // Connect the client to the server (optional starting in v4.7)
        await client.connect();

        // Establish and verify connection
        await client.db("admin").command({ping: 1});
        console.log("Connected successfully to server");

        let collectionName = `payments--${Date.now()}`
        let payments = client.db(collectionName).collection(collectionName)

        // ensure no payments at the moment
        assert(await payments.countDocuments() === 0)

        // initialization of object
        await cas_upsert(payments, {
            "tocoPaymentId": DEFAULT_PAYMENT_ID, //internal id
            "gatewayPaymentId": uuidv4(), //external id given by gateway
            "timeOfPayment": Date.now(), //moment of fiat's transaction completion
            "userAccountId": "alice@toco", // user's wallet account name
            "amount": 100, // amount of money charged from the user
        },
            "NEW"
        );

        await cas_upsert(payments,
            {},
            "WAITING_FOR_SENDING"
        );

        await cas_upsert(payments,
            {},
            "SENT_TO_POOL"
        );

        // await cas_upsert(payments,
        //     {},
        //     "SENT_TO_POOL"
        // );
        //
        // await cas_upsert(payments,
        //     {},
        //     "SUCCEED"
        // );
        //
        // await cas_upsert(payments,
        //     {},
        //     "FAILED"
        // );
        //
        // await cas_upsert(payments,
        //     {},
        //     "NEW"
        // );
    } finally {
        // Ensures that the client will close when you finish/error
        await client.close();
    }
}

async function cas_upsert(collection, updateFilters, new_status) {
    let filterPaymentId = {"tocoPaymentId": DEFAULT_PAYMENT_ID};
    let now = Date.now();
    await collection.updateOne(
        filterPaymentId,
        [
            {
                $set: updateFilters
            },
            {
                $set: {
                    "status": {
                        $cond: [ { $not: {$in: ["$status", FINAL_STATUSES]}}, new_status, "$status"]
                    },
                    // set `createdAt` only if it is insert operation
                    "createdAt": {
                        $ifNull: ["$createdAt", now]
                    },
                    "updatedAt": now
                }
            },
        ],
        {
            upsert: true,
        }
    )
    console.log(`${JSON.stringify(await collection.findOne(filterPaymentId))}`)
}

run().catch(console.dir);
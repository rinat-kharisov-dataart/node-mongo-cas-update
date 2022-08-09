const {MongoClient} = require("mongodb");
const assert = require("assert");
const {v4: uuidv4} = require('uuid');

// Connection URI
// run mongo with command `docker run  -e MONGO_INITDB_ROOT_USERNAME=admin -e MONGO_INITDB_ROOT_PASSWORD=pass -p 27017:27017 mongo:latest`
const uri = "mongodb://admin:pass@localhost";
// Create a new MongoClient
const client = new MongoClient(uri);

const DEFAULT_PAYMENT_ID = "payment123456";

// NEW -> PENDING -> SENT -> SUCCEED | FAILED
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
        await cas_status_upsert(payments, {
                "tocoPaymentId": DEFAULT_PAYMENT_ID, //internal id
                "userAccountId": "alice@toco", // user's wallet account name
            },
            [],
            "NEW"
        );

        // task to transfer received
        await cas_status_upsert(payments, {
                "tocoPaymentId": DEFAULT_PAYMENT_ID, //internal id
                "gatewayPaymentId": uuidv4(), //external id given by gateway
                "timeOfPayment": Date.now(), //moment of fiat's transaction completion
                "amount": 100, // amount of money charged from the user
            },
            ["NEW"],
            "PENDING"
        );

        // extrinsic was sent
        await cas_status_upsert(payments,
            {},
            ["NEW", "PENDING"],
            "SENT"
        );

        // accidentally comsumed task one more time
        await cas_status_upsert(payments, {
                "tocoPaymentId": DEFAULT_PAYMENT_ID, //internal id
                "gatewayPaymentId": uuidv4(), //external id given by gateway
                "timeOfPayment": Date.now(), //moment of fiat's transaction completion
                "amount": 100, // amount of money charged from the user
            },
            ["NEW"],
            "PENDING"
        );

        // event was consumed from the blockchain
        await cas_status_upsert(payments,
            {},
            ["NEW", "PENDING", "SENT"],
            "SUCCEED"
        );

        await cas_status_upsert(payments,
            {},
            ["PENDING"],
            ["SENT"]
        );

        await cas_status_upsert(payments,
            {},
            ["NEW", "PENDING", "SENT", "SUCCEED"],
            "FAILED"
        );

    } finally {
        // Ensures that the client will close when you finish/error
        await client.close();
    }
}

async function cas_status_upsert(collection, updateFilters, expected_statuses, new_status) {
    let filterPaymentId = {"tocoPaymentId": DEFAULT_PAYMENT_ID};
    let now = Date.now();
    let mutatedTransfer = await collection.findOneAndUpdate(
        filterPaymentId,
        [
            {
                $set: updateFilters
            },
            {
                $set: {
                    "status": {
                        // set new status only if current status is not set or current status one of expected
                        $cond: [{$or: [{$not: ["$status"]}, {$in: ["$status", expected_statuses]}]}, new_status, "$status"]
                    },
                    "createdAt": {
                        // set `createdAt` only if it is insert operation
                        $ifNull: ["$createdAt", now]
                    },
                    "updatedAt": now
                }
            },
        ],
        {
            upsert: true,
            returnDocument: "after"
        }
    )
    console.log(`${JSON.stringify(mutatedTransfer.value)}`)
    mutatedTransfer.value
}

run().catch(console.dir);
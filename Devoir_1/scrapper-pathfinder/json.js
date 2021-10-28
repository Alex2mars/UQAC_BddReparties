const {MongoClient} = require("mongodb");
const uri = "mongodb+srv://user:root@cluster0.v4hqy.mongodb.net/scrapping?retryWrites=true&w=majority";
const client = new MongoClient(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

const main = async (client) => {
    try {
        await client.connect()
        const cursor = client.db("scrapping").collection("spell").find({
            "classes": {
                $elemMatch: {
                    $and: [{
                        "class": {
                            $in: ["wizard", "sorcerer"],
                        },
                        "level": {$lte: 4}
                    }]
                }
            },
            "casting": {
                $elemMatch: {
                    "value": "V",
                },
            },
            "casting.value": {
                $not: {
                    $type: 'array'
                }
            }
        })

        const data = await cursor.toArray()
        const title = data.map(item => {
            return item.title
        })
        console.log(title.join(" | "))
        console.log(data.length)

    } catch (err) {
        console.error(err)
    } finally {
        await client.close()
    }
}

main(client).catch(console.error)

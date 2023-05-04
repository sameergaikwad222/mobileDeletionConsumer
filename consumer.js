const config = require("./config.json")[process.env.NODE_ENV || "dev"];
const { dbConnect } = require("./database/dbConnect");
var amqp = require("amqplib");

async function consumeMobileDeletionEvent() {
  try {
    const queue = config.mobileDeletionQueue;
    const connection = await amqp.connect(config.rabbit.url);
    console.log("RabbitMq Connected Successfully!");
    const channel = await connection.createChannel();
    channel.assertQueue(queue, { durable: true });
    channel.prefetch(1);
    let sequelize = await dbConnect();
    channel.consume(
      queue,
      async (msg) => {
        let mobile = JSON.parse(msg.content.toString());
        mobile = +mobile;
        //Search for user against mobile
        const [res1, meta] = await sequelize.query(
          `select * from kyc where mobile='${mobile}';`
        );
        let user = res1[0];

        //Delete user if found..

        if (user) {
          // -------*****************Delete the user data*************---------------------
          const [_, metadata] = await sequelize.query(
            `delete from kyc where mobile='${user.mobile}';`
          );

          if (metadata.rowCount > 0) {
            console.log(
              "Successfully deleted user ... ",
              msg.content.toString()
            );
            channel.ack(msg);
          } else {
            console.log(
              "User deletion issue found for ",
              msg.content.toString()
            );
            channel.nack(msg);
          }
          //Write producer code here.....
        } else {
          console.log("No user found for ", msg.content.toString());
          channel.ack(msg);
        }
      },
      {
        noAck: false,
      }
    );
  } catch (error) {
    console.log(`Error Here: ${error}`);
    process.exit(1);
  }
}

consumeMobileDeletionEvent();

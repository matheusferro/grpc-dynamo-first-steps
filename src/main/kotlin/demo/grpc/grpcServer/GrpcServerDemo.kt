package demo.grpc.grpcServer

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.Table
import demo.grpc.DemoGrpcTestsRequest
import demo.grpc.DemoGrpcTestsResponse
import demo.grpc.DemoGrpcTestsServiceGrpc
import io.grpc.stub.StreamObserver
import java.math.BigInteger
import javax.inject.Singleton

@Singleton
class GrpcServerDemo : DemoGrpcTestsServiceGrpc.DemoGrpcTestsServiceImplBase() {

    override fun saveDynamoDb(
        request: DemoGrpcTestsRequest?,
        responseObserver: StreamObserver<DemoGrpcTestsResponse>?
    ) {
        responseObserver ?: throw RuntimeException()
        request ?: throw RuntimeException()

        //Criando Client Builder
        val clientBuilder = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "local"))
            .build()

        //Criando o client
        val clientDB = DynamoDB(clientBuilder)

        //Meio para listar tabelas já criadas.
        val tables = clientDB.listTables()
        println(tables.map {
            it.tableName.toString()
        })

        //Criando itens para ser inserido no banco de dados.
        val item: Item = Item()
            .withPrimaryKey("pk_teste", "1") //Campo obrigátorio
            .withPrimaryKey("sk_teste", "sk#1") //Campo obrigátorio
            .withJSON(
                "info",
                """
                    {"testeParam": "Valor",
                    "teste2": "Adicionado um valor"}
                """.trimIndent()
            )

        val item2: Item = Item()
            .withPrimaryKey("pk_teste", "2") //Campo obrigátorio
            .withPrimaryKey("sk_teste", "sk#3") //Campo obrigátorio
            .withJSON(
                "info",
                """
                    {"testeParam": "Valor",
                    "teste2": "Adicionado um valor"}
                """.trimIndent()
            )
            .withString("nome", "Matheus")
            .withBigInteger("idade", BigInteger.valueOf(19))
            .withJSON(
                "credenciais",
                """
                    {
                        "email": "email@gmail.com",
                        "senha": "senhaSegura"
                    }
                """.trimIndent()
            )

        //Pegando a tabela para inserir os itens
        val table: Table = clientDB.getTable("tb_teste23")

        //Inserindo os itens
        table.putItem(item)
        table.putItem(item2)

        responseObserver.onCompleted()
    }

}
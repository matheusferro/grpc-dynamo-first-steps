package demo.grpc.grpcServer

import demo.grpc.DemoGrpcTestsRequest
import demo.grpc.DemoGrpcTestsResponse
import demo.grpc.DemoGrpcTestsServiceGrpc
import io.grpc.stub.StreamObserver
import javax.inject.Singleton

@Singleton
class GrpcServerDemo : DemoGrpcTestsServiceGrpc.DemoGrpcTestsServiceImplBase() {

    override fun saveDynamoDb(
        request: DemoGrpcTestsRequest?,
        responseObserver: StreamObserver<DemoGrpcTestsResponse>?
    ) {
        responseObserver ?: throw RuntimeException()
        request ?: throw RuntimeException()

        val response = DemoGrpcTestsResponse
            .newBuilder()
            .setMessage(
                request.name + " - " +
                        request.email + " - " +
                        request.phoneList
            )
            .build()

        responseObserver.onNext(response)
        responseObserver.onCompleted()
    }

}
package example

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"log"
)

type DynamicRoute struct {
	Prefix string
	Url    string
}

func FetchRoutes() []DynamicRoute {

	// Initialize a session that the SDK will use to load
	// credentials from the shared credentials file ~/.aws/credentials
	sess, err := session.NewSession()

	if err != nil {
		return nil
	}

	// Create DynamoDB client
	svc := dynamodb.New(sess)

	proj := expression.NamesList(expression.Name("prefix"), expression.Name("url"))
	expr, err := expression.NewBuilder().WithProjection(proj).Build()
	if err != nil {
		log.Fatalf("Got error building expression: %s", err)
	}

	// Build the query input parameters
	params := &dynamodb.ScanInput{
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		FilterExpression:          expr.Filter(),
		ProjectionExpression:      expr.Projection(),
		TableName:                 aws.String("routes"),
	}

	result, err := svc.Scan(params)
	if err != nil {
		log.Fatalf("Query API call failed: %s", err)
	}

	var results []DynamicRoute
	for _, i := range result.Items {
		dynamicRoute := DynamicRoute{}

		err = dynamodbattribute.UnmarshalMap(i, &dynamicRoute)

		if err != nil {
			log.Fatalf("Got error unmarshalling: %s", err)
		}

		results = append(results, dynamicRoute)
	}

	return results
}

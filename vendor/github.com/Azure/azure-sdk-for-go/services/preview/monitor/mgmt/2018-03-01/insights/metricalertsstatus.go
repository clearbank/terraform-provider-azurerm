package insights

// Copyright (c) Microsoft and contributors.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Code generated by Microsoft (R) AutoRest Code Generator.
// Changes may cause incorrect behavior and will be lost if the code is regenerated.

import (
	"context"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/tracing"
	"net/http"
)

// MetricAlertsStatusClient is the monitor Management Client
type MetricAlertsStatusClient struct {
	BaseClient
}

// NewMetricAlertsStatusClient creates an instance of the MetricAlertsStatusClient client.
func NewMetricAlertsStatusClient(subscriptionID string) MetricAlertsStatusClient {
	return NewMetricAlertsStatusClientWithBaseURI(DefaultBaseURI, subscriptionID)
}

// NewMetricAlertsStatusClientWithBaseURI creates an instance of the MetricAlertsStatusClient client.
func NewMetricAlertsStatusClientWithBaseURI(baseURI string, subscriptionID string) MetricAlertsStatusClient {
	return MetricAlertsStatusClient{NewWithBaseURI(baseURI, subscriptionID)}
}

// List retrieve an alert rule status.
// Parameters:
// resourceGroupName - the name of the resource group.
// ruleName - the name of the rule.
func (client MetricAlertsStatusClient) List(ctx context.Context, resourceGroupName string, ruleName string) (result MetricAlertStatusCollection, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/MetricAlertsStatusClient.List")
		defer func() {
			sc := -1
			if result.Response.Response != nil {
				sc = result.Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	req, err := client.ListPreparer(ctx, resourceGroupName, ruleName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "insights.MetricAlertsStatusClient", "List", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "insights.MetricAlertsStatusClient", "List", resp, "Failure sending request")
		return
	}

	result, err = client.ListResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "insights.MetricAlertsStatusClient", "List", resp, "Failure responding to request")
	}

	return
}

// ListPreparer prepares the List request.
func (client MetricAlertsStatusClient) ListPreparer(ctx context.Context, resourceGroupName string, ruleName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"ruleName":          autorest.Encode("path", ruleName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2018-03-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Insights/metricAlerts/{ruleName}/status", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ListSender sends the List request. The method will close the
// http.Response Body if it receives an error.
func (client MetricAlertsStatusClient) ListSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// ListResponder handles the response to the List request. The method always
// closes the http.Response Body.
func (client MetricAlertsStatusClient) ListResponder(resp *http.Response) (result MetricAlertStatusCollection, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListByName retrieve an alert rule status.
// Parameters:
// resourceGroupName - the name of the resource group.
// ruleName - the name of the rule.
// statusName - the name of the status.
func (client MetricAlertsStatusClient) ListByName(ctx context.Context, resourceGroupName string, ruleName string, statusName string) (result MetricAlertStatusCollection, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/MetricAlertsStatusClient.ListByName")
		defer func() {
			sc := -1
			if result.Response.Response != nil {
				sc = result.Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	req, err := client.ListByNamePreparer(ctx, resourceGroupName, ruleName, statusName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "insights.MetricAlertsStatusClient", "ListByName", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListByNameSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "insights.MetricAlertsStatusClient", "ListByName", resp, "Failure sending request")
		return
	}

	result, err = client.ListByNameResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "insights.MetricAlertsStatusClient", "ListByName", resp, "Failure responding to request")
	}

	return
}

// ListByNamePreparer prepares the ListByName request.
func (client MetricAlertsStatusClient) ListByNamePreparer(ctx context.Context, resourceGroupName string, ruleName string, statusName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"ruleName":          autorest.Encode("path", ruleName),
		"statusName":        autorest.Encode("path", statusName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2018-03-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Insights/metricAlerts/{ruleName}/status/{statusName}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ListByNameSender sends the ListByName request. The method will close the
// http.Response Body if it receives an error.
func (client MetricAlertsStatusClient) ListByNameSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// ListByNameResponder handles the response to the ListByName request. The method always
// closes the http.Response Body.
func (client MetricAlertsStatusClient) ListByNameResponder(resp *http.Response) (result MetricAlertStatusCollection, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

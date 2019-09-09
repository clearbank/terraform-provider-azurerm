package frontdoor

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
	"github.com/Azure/go-autorest/autorest/validation"
	"github.com/Azure/go-autorest/tracing"
	"net/http"
)

// HealthProbeSettingsClient is the frontDoor Client
type HealthProbeSettingsClient struct {
	BaseClient
}

// NewHealthProbeSettingsClient creates an instance of the HealthProbeSettingsClient client.
func NewHealthProbeSettingsClient(subscriptionID string) HealthProbeSettingsClient {
	return NewHealthProbeSettingsClientWithBaseURI(DefaultBaseURI, subscriptionID)
}

// NewHealthProbeSettingsClientWithBaseURI creates an instance of the HealthProbeSettingsClient client.
func NewHealthProbeSettingsClientWithBaseURI(baseURI string, subscriptionID string) HealthProbeSettingsClient {
	return HealthProbeSettingsClient{NewWithBaseURI(baseURI, subscriptionID)}
}

// CreateOrUpdate creates a new HealthProbeSettings with the specified Rule name within the specified Front Door.
// Parameters:
// resourceGroupName - name of the Resource group within the Azure subscription.
// frontDoorName - name of the Front Door which is globally unique.
// healthProbeSettingsName - name of the health probe settings which is unique within the Front Door.
// healthProbeSettingsParameters - healthProbeSettings properties needed to create a new Front Door.
func (client HealthProbeSettingsClient) CreateOrUpdate(ctx context.Context, resourceGroupName string, frontDoorName string, healthProbeSettingsName string, healthProbeSettingsParameters HealthProbeSettingsModel) (result HealthProbeSettingsCreateOrUpdateFuture, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/HealthProbeSettingsClient.CreateOrUpdate")
		defer func() {
			sc := -1
			if result.Response() != nil {
				sc = result.Response().StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 80, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9_\-\(\)\.]*[^\.]$`, Chain: nil}}},
		{TargetValue: frontDoorName,
			Constraints: []validation.Constraint{{Target: "frontDoorName", Name: validation.MaxLength, Rule: 64, Chain: nil},
				{Target: "frontDoorName", Name: validation.MinLength, Rule: 5, Chain: nil},
				{Target: "frontDoorName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9]+([-a-zA-Z0-9]?[a-zA-Z0-9])*$`, Chain: nil}}},
		{TargetValue: healthProbeSettingsName,
			Constraints: []validation.Constraint{{Target: "healthProbeSettingsName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "healthProbeSettingsName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "healthProbeSettingsName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9]+(-*[a-zA-Z0-9])*$`, Chain: nil}}}}); err != nil {
		return result, validation.NewError("frontdoor.HealthProbeSettingsClient", "CreateOrUpdate", err.Error())
	}

	req, err := client.CreateOrUpdatePreparer(ctx, resourceGroupName, frontDoorName, healthProbeSettingsName, healthProbeSettingsParameters)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "CreateOrUpdate", nil, "Failure preparing request")
		return
	}

	result, err = client.CreateOrUpdateSender(req)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "CreateOrUpdate", result.Response(), "Failure sending request")
		return
	}

	return
}

// CreateOrUpdatePreparer prepares the CreateOrUpdate request.
func (client HealthProbeSettingsClient) CreateOrUpdatePreparer(ctx context.Context, resourceGroupName string, frontDoorName string, healthProbeSettingsName string, healthProbeSettingsParameters HealthProbeSettingsModel) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"frontDoorName":           autorest.Encode("path", frontDoorName),
		"healthProbeSettingsName": autorest.Encode("path", healthProbeSettingsName),
		"resourceGroupName":       autorest.Encode("path", resourceGroupName),
		"subscriptionId":          autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2019-04-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	healthProbeSettingsParameters.Type = nil
	preparer := autorest.CreatePreparer(
		autorest.AsContentType("application/json; charset=utf-8"),
		autorest.AsPut(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Network/frontDoors/{frontDoorName}/healthProbeSettings/{healthProbeSettingsName}", pathParameters),
		autorest.WithJSON(healthProbeSettingsParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// CreateOrUpdateSender sends the CreateOrUpdate request. The method will close the
// http.Response Body if it receives an error.
func (client HealthProbeSettingsClient) CreateOrUpdateSender(req *http.Request) (future HealthProbeSettingsCreateOrUpdateFuture, err error) {
	var resp *http.Response
	resp, err = autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
	if err != nil {
		return
	}
	future.Future, err = azure.NewFutureFromResponse(resp)
	return
}

// CreateOrUpdateResponder handles the response to the CreateOrUpdate request. The method always
// closes the http.Response Body.
func (client HealthProbeSettingsClient) CreateOrUpdateResponder(resp *http.Response) (result HealthProbeSettingsModel, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusCreated, http.StatusAccepted),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// Delete deletes an existing HealthProbeSettings with the specified parameters.
// Parameters:
// resourceGroupName - name of the Resource group within the Azure subscription.
// frontDoorName - name of the Front Door which is globally unique.
// healthProbeSettingsName - name of the health probe settings which is unique within the Front Door.
func (client HealthProbeSettingsClient) Delete(ctx context.Context, resourceGroupName string, frontDoorName string, healthProbeSettingsName string) (result HealthProbeSettingsDeleteFuture, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/HealthProbeSettingsClient.Delete")
		defer func() {
			sc := -1
			if result.Response() != nil {
				sc = result.Response().StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 80, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9_\-\(\)\.]*[^\.]$`, Chain: nil}}},
		{TargetValue: frontDoorName,
			Constraints: []validation.Constraint{{Target: "frontDoorName", Name: validation.MaxLength, Rule: 64, Chain: nil},
				{Target: "frontDoorName", Name: validation.MinLength, Rule: 5, Chain: nil},
				{Target: "frontDoorName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9]+([-a-zA-Z0-9]?[a-zA-Z0-9])*$`, Chain: nil}}},
		{TargetValue: healthProbeSettingsName,
			Constraints: []validation.Constraint{{Target: "healthProbeSettingsName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "healthProbeSettingsName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "healthProbeSettingsName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9]+(-*[a-zA-Z0-9])*$`, Chain: nil}}}}); err != nil {
		return result, validation.NewError("frontdoor.HealthProbeSettingsClient", "Delete", err.Error())
	}

	req, err := client.DeletePreparer(ctx, resourceGroupName, frontDoorName, healthProbeSettingsName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "Delete", nil, "Failure preparing request")
		return
	}

	result, err = client.DeleteSender(req)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "Delete", result.Response(), "Failure sending request")
		return
	}

	return
}

// DeletePreparer prepares the Delete request.
func (client HealthProbeSettingsClient) DeletePreparer(ctx context.Context, resourceGroupName string, frontDoorName string, healthProbeSettingsName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"frontDoorName":           autorest.Encode("path", frontDoorName),
		"healthProbeSettingsName": autorest.Encode("path", healthProbeSettingsName),
		"resourceGroupName":       autorest.Encode("path", resourceGroupName),
		"subscriptionId":          autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2019-04-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsDelete(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Network/frontDoors/{frontDoorName}/healthProbeSettings/{healthProbeSettingsName}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// DeleteSender sends the Delete request. The method will close the
// http.Response Body if it receives an error.
func (client HealthProbeSettingsClient) DeleteSender(req *http.Request) (future HealthProbeSettingsDeleteFuture, err error) {
	var resp *http.Response
	resp, err = autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
	if err != nil {
		return
	}
	future.Future, err = azure.NewFutureFromResponse(resp)
	return
}

// DeleteResponder handles the response to the Delete request. The method always
// closes the http.Response Body.
func (client HealthProbeSettingsClient) DeleteResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted, http.StatusNoContent),
		autorest.ByClosing())
	result.Response = resp
	return
}

// Get gets a HealthProbeSettings with the specified Rule name within the specified Front Door.
// Parameters:
// resourceGroupName - name of the Resource group within the Azure subscription.
// frontDoorName - name of the Front Door which is globally unique.
// healthProbeSettingsName - name of the health probe settings which is unique within the Front Door.
func (client HealthProbeSettingsClient) Get(ctx context.Context, resourceGroupName string, frontDoorName string, healthProbeSettingsName string) (result HealthProbeSettingsModel, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/HealthProbeSettingsClient.Get")
		defer func() {
			sc := -1
			if result.Response.Response != nil {
				sc = result.Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 80, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9_\-\(\)\.]*[^\.]$`, Chain: nil}}},
		{TargetValue: frontDoorName,
			Constraints: []validation.Constraint{{Target: "frontDoorName", Name: validation.MaxLength, Rule: 64, Chain: nil},
				{Target: "frontDoorName", Name: validation.MinLength, Rule: 5, Chain: nil},
				{Target: "frontDoorName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9]+([-a-zA-Z0-9]?[a-zA-Z0-9])*$`, Chain: nil}}},
		{TargetValue: healthProbeSettingsName,
			Constraints: []validation.Constraint{{Target: "healthProbeSettingsName", Name: validation.MaxLength, Rule: 90, Chain: nil},
				{Target: "healthProbeSettingsName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "healthProbeSettingsName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9]+(-*[a-zA-Z0-9])*$`, Chain: nil}}}}); err != nil {
		return result, validation.NewError("frontdoor.HealthProbeSettingsClient", "Get", err.Error())
	}

	req, err := client.GetPreparer(ctx, resourceGroupName, frontDoorName, healthProbeSettingsName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "Get", nil, "Failure preparing request")
		return
	}

	resp, err := client.GetSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "Get", resp, "Failure sending request")
		return
	}

	result, err = client.GetResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "Get", resp, "Failure responding to request")
	}

	return
}

// GetPreparer prepares the Get request.
func (client HealthProbeSettingsClient) GetPreparer(ctx context.Context, resourceGroupName string, frontDoorName string, healthProbeSettingsName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"frontDoorName":           autorest.Encode("path", frontDoorName),
		"healthProbeSettingsName": autorest.Encode("path", healthProbeSettingsName),
		"resourceGroupName":       autorest.Encode("path", resourceGroupName),
		"subscriptionId":          autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2019-04-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Network/frontDoors/{frontDoorName}/healthProbeSettings/{healthProbeSettingsName}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// GetSender sends the Get request. The method will close the
// http.Response Body if it receives an error.
func (client HealthProbeSettingsClient) GetSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// GetResponder handles the response to the Get request. The method always
// closes the http.Response Body.
func (client HealthProbeSettingsClient) GetResponder(resp *http.Response) (result HealthProbeSettingsModel, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// ListByFrontDoor lists all of the HealthProbeSettings within a Front Door.
// Parameters:
// resourceGroupName - name of the Resource group within the Azure subscription.
// frontDoorName - name of the Front Door which is globally unique.
func (client HealthProbeSettingsClient) ListByFrontDoor(ctx context.Context, resourceGroupName string, frontDoorName string) (result HealthProbeSettingsListResultPage, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/HealthProbeSettingsClient.ListByFrontDoor")
		defer func() {
			sc := -1
			if result.hpslr.Response.Response != nil {
				sc = result.hpslr.Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	if err := validation.Validate([]validation.Validation{
		{TargetValue: resourceGroupName,
			Constraints: []validation.Constraint{{Target: "resourceGroupName", Name: validation.MaxLength, Rule: 80, Chain: nil},
				{Target: "resourceGroupName", Name: validation.MinLength, Rule: 1, Chain: nil},
				{Target: "resourceGroupName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9_\-\(\)\.]*[^\.]$`, Chain: nil}}},
		{TargetValue: frontDoorName,
			Constraints: []validation.Constraint{{Target: "frontDoorName", Name: validation.MaxLength, Rule: 64, Chain: nil},
				{Target: "frontDoorName", Name: validation.MinLength, Rule: 5, Chain: nil},
				{Target: "frontDoorName", Name: validation.Pattern, Rule: `^[a-zA-Z0-9]+([-a-zA-Z0-9]?[a-zA-Z0-9])*$`, Chain: nil}}}}); err != nil {
		return result, validation.NewError("frontdoor.HealthProbeSettingsClient", "ListByFrontDoor", err.Error())
	}

	result.fn = client.listByFrontDoorNextResults
	req, err := client.ListByFrontDoorPreparer(ctx, resourceGroupName, frontDoorName)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "ListByFrontDoor", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListByFrontDoorSender(req)
	if err != nil {
		result.hpslr.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "ListByFrontDoor", resp, "Failure sending request")
		return
	}

	result.hpslr, err = client.ListByFrontDoorResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "ListByFrontDoor", resp, "Failure responding to request")
	}

	return
}

// ListByFrontDoorPreparer prepares the ListByFrontDoor request.
func (client HealthProbeSettingsClient) ListByFrontDoorPreparer(ctx context.Context, resourceGroupName string, frontDoorName string) (*http.Request, error) {
	pathParameters := map[string]interface{}{
		"frontDoorName":     autorest.Encode("path", frontDoorName),
		"resourceGroupName": autorest.Encode("path", resourceGroupName),
		"subscriptionId":    autorest.Encode("path", client.SubscriptionID),
	}

	const APIVersion = "2019-04-01"
	queryParameters := map[string]interface{}{
		"api-version": APIVersion,
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithBaseURL(client.BaseURI),
		autorest.WithPathParameters("/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Network/frontDoors/{frontDoorName}/healthProbeSettings", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	return preparer.Prepare((&http.Request{}).WithContext(ctx))
}

// ListByFrontDoorSender sends the ListByFrontDoor request. The method will close the
// http.Response Body if it receives an error.
func (client HealthProbeSettingsClient) ListByFrontDoorSender(req *http.Request) (*http.Response, error) {
	return autorest.SendWithSender(client, req,
		azure.DoRetryWithRegistration(client.Client))
}

// ListByFrontDoorResponder handles the response to the ListByFrontDoor request. The method always
// closes the http.Response Body.
func (client HealthProbeSettingsClient) ListByFrontDoorResponder(resp *http.Response) (result HealthProbeSettingsListResult, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

// listByFrontDoorNextResults retrieves the next set of results, if any.
func (client HealthProbeSettingsClient) listByFrontDoorNextResults(ctx context.Context, lastResults HealthProbeSettingsListResult) (result HealthProbeSettingsListResult, err error) {
	req, err := lastResults.healthProbeSettingsListResultPreparer(ctx)
	if err != nil {
		return result, autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "listByFrontDoorNextResults", nil, "Failure preparing next results request")
	}
	if req == nil {
		return
	}
	resp, err := client.ListByFrontDoorSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		return result, autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "listByFrontDoorNextResults", resp, "Failure sending next results request")
	}
	result, err = client.ListByFrontDoorResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "frontdoor.HealthProbeSettingsClient", "listByFrontDoorNextResults", resp, "Failure responding to next results request")
	}
	return
}

// ListByFrontDoorComplete enumerates all values, automatically crossing page boundaries as required.
func (client HealthProbeSettingsClient) ListByFrontDoorComplete(ctx context.Context, resourceGroupName string, frontDoorName string) (result HealthProbeSettingsListResultIterator, err error) {
	if tracing.IsEnabled() {
		ctx = tracing.StartSpan(ctx, fqdn+"/HealthProbeSettingsClient.ListByFrontDoor")
		defer func() {
			sc := -1
			if result.Response().Response.Response != nil {
				sc = result.page.Response().Response.Response.StatusCode
			}
			tracing.EndSpan(ctx, sc, err)
		}()
	}
	result.page, err = client.ListByFrontDoor(ctx, resourceGroupName, frontDoorName)
	return
}

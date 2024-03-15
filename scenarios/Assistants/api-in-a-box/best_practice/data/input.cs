// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// ------------------------------------------------------------

namespace AzureImpactRP.Controllers.WorkloadImpacts
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Threading.Tasks;
    using AzImpactRP.Models.WorkloadImpacts;
    using AzureImpactRP.Client;
    using AzureImpactRP.Helpers;
    using AzureImpactRP.Models;
    using AzureImpactRP.Utils;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Http.Features;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Newtonsoft.Json.Serialization;

    /// <summary>
    /// Base controller that holds common methods to be used by child controllers
    /// for different APIs.
    /// </summary>
    public class BaseController : Controller
    {
        private readonly IEventHubConnectorClient impactIngestEventHubClient;

        private readonly Dictionary<string, Dictionary<string, List<Config>>> allowedCategories;

        private readonly ImpactCategoryConfigHelper impactCategoryConfigHelper;

        private readonly List<ImpactCategory> impactCategories;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseController"/> class.
        /// </summary>
        /// <param name="logger">Logger.</param>
        /// <param name="impactIngestEventHubClient">Event hub client.</param>
        public BaseController(IEventHubConnectorClient impactIngestEventHubClient, ImpactCategoryConfigHelper impactCategoryConfigHelper)
        {
            this.impactIngestEventHubClient = impactIngestEventHubClient;
            this.impactCategoryConfigHelper = impactCategoryConfigHelper;
            allowedCategories = this.impactCategoryConfigHelper.AllowedCategories;
            impactCategories = this.impactCategoryConfigHelper.ImpactCategories;
        }

        /// <summary>
        /// Method to report an impact. This method internally invokes SendEvent method that will
        /// send the impact to event hub to finally store it in Kusto.
        /// </summary>
        /// <param name="impact"></param>
        /// <param name="uniqueImpactId"></param>
        /// <param name="apiVersion"></param>
        /// <param name="logger"></param>
        /// <param name="appId"></param>
        /// <param name="requestId"></param>
        /// <param name="correlationId"></param>
        /// <param name="tenantId"></param>
        /// <param name="stopWatch"></param>
        /// <param name="isInternal"></param>
        /// <returns></returns>
        [NonAction]
        private async Task<IActionResult> ReportImpactAsync(Impact impact, Guid uniqueImpactId, string apiVersion
            , ILogger logger, string appId, string requestId, string correlationId
            , string tenantId, Stopwatch stopWatch, bool isInternal, Nullable<Guid> parentImpactId)
        {
            logger.LogTrace(">> Start: ReportImpactAsync()");
            impact.Properties[Constants.ImpactUniqueId] = uniqueImpactId.ToString();
            impact.Properties[Constants.ReportedTimeUtc] = DateTime.Now.ToUniversalTime();
            impact.ApiVersion = apiVersion;
            impact.IsInternal = isInternal;

            // validate if the appID is valid Guid. If it's a guid only then it will be stored in Kusto.
            bool isValidAppId = Validator.IsValidGuid(appId);
            if (isValidAppId)
            {
                // if it's guid only then store in database
                impact.AppId = Guid.Parse(appId);
            }

            string response = JsonConvert.SerializeObject(impact, new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() });
            try
            {
                await this.SendEvent(logger, impactIngestEventHubClient, impact, uniqueImpactId, tenantId, requestId, correlationId, parentImpactId);
                logger.LogInformation($"Reported impact with ImpactId: {uniqueImpactId}, Request Id: {requestId} and Correlation Id: {correlationId} at {impact.Properties[Constants.ReportedTimeUtc]}");
                logger.LogTrace("<< End: ReportImpactAsync()");
                stopWatch.Stop();
                logger.LogInformation($"Ending impact creation request at: {DateTime.Now.ToUniversalTime()}");
                logger.LogInformation($"Total time taken to process request: {stopWatch.ElapsedMilliseconds} ms.");
                return Content(response, "application/json");
            }
            catch (Exception ex)
            {
                logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Encountered an error in sending impact event to event hub. Error: {ex.StackTrace}");
                throw ex;
            }
        }

        /// <summary>
        /// Method to send impact event details to event hub and store in Kusto.
        /// </summary>
        /// <param name="impact">Reported impact model</param>
        /// <param name="uniqueImpactId">Guid of the reported impact</param>
        /// <returns></returns>
        [NonAction]
        private async Task SendEvent(ILogger logger, IEventHubConnectorClient impactIngestEventHubClient, Impact impact, Guid uniqueImpactId, string tenantId, string requestId, string correlationId, Nullable<Guid> parentImpactId)
        {
            logger.LogTrace(">> Start: SendEvent()");
            try
            {
                var eventHubNamespace = Environment.GetEnvironmentVariable(Constants.EventHubNamespace);
                var eventHub = Environment.GetEnvironmentVariable(Constants.EventHub);
                logger.LogInformation($"Sending event with ImpactId: {uniqueImpactId} to eventHubNamespace: {eventHubNamespace}, eventHub: {eventHub} for request with RequestId: {requestId}, correlationId: {correlationId}");
                var eventHubClient = await impactIngestEventHubClient.GetEventHubConnectorClient(eventHubNamespace, eventHub).ConfigureAwait(true);
                var impactData = CreateImpactEvent(logger, impact, uniqueImpactId, tenantId, requestId, correlationId, parentImpactId);
                await eventHubClient.SendEvent(impactData, uniqueImpactId, requestId, correlationId).ConfigureAwait(true);
                logger.LogInformation($"Impact with id: {uniqueImpactId} sent to event hub at {DateTime.Now}");
            }
            catch (Exception ex)
            {
                logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId} Error while sending event with ImpactId: {uniqueImpactId} to event hub: {ex.StackTrace}");
                throw ex;
            }

            logger.LogTrace("<< End: SendEvent()");
        }

        /// <summary>
        /// Method to get the impact data to be sent to Event hub.
        /// </summary>
        /// <param name="impact">The impact object that holds the data.</param>
        /// <returns>Impact paylod that will be sent to event hub and stored in Kusto</returns>
        [NonAction]
        private string CreateImpactEvent(ILogger logger, Impact impact, Guid uniqueImpactId, string tenantId, string requestId, string correlationId, Nullable<Guid> parentImpactId)
        {
            logger.LogTrace(">> Start: CreateImpactEvent()");
            logger.LogInformation($"Creating ImpactEvent with impactId: {uniqueImpactId} for Request with Id: {requestId} and CorrelationId: {correlationId}");
            try
            {
                DateTime startDateTime = Convert.ToDateTime(impact.Properties[Constants.StartDateTime]);
                var endDate = impact.Properties[Constants.EndDateTime];
                DateTime? endDateTime = endDate != null && endDate.Type != JTokenType.Null ? Convert.ToDateTime(endDate).ToUniversalTime() : null;
                ImpactEvent impactEvent = new ImpactEvent()
                {
                    Id = uniqueImpactId,
                    ArmId = impact.Id,
                    ImpactType = impact.Type,
                    TenantId = tenantId,
                    Name = impact.Name,
                    StartDateTimeUtc = startDateTime.ToUniversalTime(),
                    EndDateTimeUtc = endDateTime,
                    ReportedTimeUtc = Convert.ToDateTime(impact.Properties[Constants.ReportedTimeUtc]),
                    Properties = JsonConvert.SerializeObject(impact.Properties),
                    ApiVersion = impact.ApiVersion,
                    IsInternal = impact.IsInternal,
                    AppId = impact.AppId,
                    ParentImpactId = parentImpactId,
                };

                string eventPayload = JsonConvert.SerializeObject(impactEvent);
                logger.LogInformation($"ImpactEvent payload for impactId: {uniqueImpactId} for Request with Id: {requestId} and CorrelationId: {correlationId} is: {eventPayload}");
                logger.LogTrace("<< End: CreateImpactEvent()");
                return eventPayload;
            }
            catch (Exception ex)
            {
                logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Error encountered creating impact event with impactId: {uniqueImpactId}, {ex}");
                throw ex;
            }
        }

        /// <summary>
        /// Method to validate input. This method will internally invoke a separate method for validation.
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="isInternal"></param>
        /// <param name="reader"></param>
        /// <param name="stopWatch"></param>
        /// <param name="subscriptionId"></param>
        /// <param name="httpContext"></param>
        /// <param name="impactCategories"></param>
        /// <param name="allowedCategories"></param>
        /// <param name="requestId"></param>
        /// <param name="correlationId"></param>
        /// <param name="apiVersion"></param>
        /// <returns></returns>
        [NonAction]
        private JsonResult ValidateInput(ILogger logger, bool isInternal
            ,StreamReader reader, Stopwatch stopWatch,
            string subscriptionId, string tenantId, HttpContext httpContext, List<ImpactCategory> impactCategories, Dictionary<string, 
                Dictionary<string, List<Config>>> allowedCategories, string requestId,
            string correlationId, string apiVersion, string input)
        {
            logger.LogTrace(">> Start: ValidateInput()");
            logger.LogInformation($"Starting validation of request input at Timestamp: {DateTime.Now}, RequestId: {requestId}, CorrelationId: {correlationId}");
            JsonResult content = this.ValidateImpactPayload(logger, httpContext, subscriptionId , tenantId
                , input, requestId, correlationId, apiVersion, impactCategories, allowedCategories, isInternal);
            if (content.StatusCode != (int)HttpStatusCode.OK)
            {
                var error = content.Value;
                stopWatch.Stop();
                logger.LogInformation($"Ending impact creation request at: {DateTime.Now.ToUniversalTime()}");
                logger.LogInformation($"Total time taken to process request: {stopWatch.ElapsedMilliseconds} ms.");
                return Helper.CreateResponse((HttpStatusCode)content.StatusCode, error);
            }

            var result = $"Successfully validated input for request: {requestId}, correlationId: {correlationId}";
            logger.LogTrace("<< End: ValidateInput()");
            return Helper.CreateResponse((HttpStatusCode)content.StatusCode, result);
        }

        /// <summary>
        /// Method that will invoke corresponding validation method based on api version.
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="httpContext"></param>
        /// <param name="subscriptionId"></param>
        /// <param name="input"></param>
        /// <param name="requestId"></param>
        /// <param name="correlationId"></param>
        /// <param name="apiVersion"></param>
        /// <param name="impactCategories"></param>
        /// <param name="allowedCategories"></param>
        /// <param name="isInternal"></param>
        /// <returns></returns>
        [NonAction]
        public virtual JsonResult ValidateImpactPayload(ILogger logger, HttpContext httpContext
            , string subscriptionId, string tenantId, string input, string requestId, string correlationId
            , string apiVersion, List<ImpactCategory> impactCategories
            , Dictionary<string, Dictionary<string, List<Config>>> allowedCategories, bool isInternal)
        {
            return Helper.CreateResponse(HttpStatusCode.BadRequest, $"Api version {apiVersion} is not supported.");
        }

        /// <summary>
        /// Method to create impacts for ARM api versions
        /// 2022-11-01-preview, 2023-02-01-preview, 2023-07-01-preview.
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="subscriptionId"></param>
        /// <param name="request"></param>
        /// <param name="isInternal"></param>
        /// <returns></returns>
        [NonAction]
        public async Task<IActionResult> CreateAndReportImpactsArmApi(ILogger logger, string subscriptionId, HttpRequest request, bool isInternal)
        {
            logger.LogTrace(">>Start: CreateAndReportImpactsArmApi()");
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            logger.LogTrace($"Received user request. Start impact creation flow at: {DateTime.Now.ToUniversalTime()}");

            // need to add this to get past the issue mentioned here https://github.com/dotnet/aspnetcore/issues/7644
            var syncIOFeature = this.HttpContext.Features.Get<IHttpBodyControlFeature>();
            if (syncIOFeature != null)
            {
                syncIOFeature.AllowSynchronousIO = true;
            }

            using (StreamReader reader = new StreamReader(request.Body))
            {
                var input = reader.ReadToEnd();
                Guid uniqueImpactId = Guid.NewGuid();
                string requestId = request.Headers[Constants.ClientRequestId].ToString();
                string correlationId = request.Headers[Constants.CorrelationId].ToString();
                string appId = request.Headers[Constants.ClientAppId].ToString();
                string apiVersion = request.Query[Constants.ApiVersion];
                string tenantId = request.Headers[Constants.TenantId].ToString();
                var result = this.ValidateInput(logger, isInternal, reader, stopWatch,
                    subscriptionId, tenantId, this.HttpContext, this.impactCategories,
                    this.allowedCategories, requestId, correlationId, apiVersion, input);
                if (result.StatusCode != (int)HttpStatusCode.OK)
                {
                    return result;
                }

                logger.LogInformation($"Creating impact from the request input at Timestamp: {DateTime.Now}, RequestId: {requestId}, CorrelationId: {correlationId}");
                Impact impact = null;
                try
                {
                    impact = Helper.GetImpact(logger, input, requestId, correlationId);
                    var uniqueId = impact.Properties[Constants.ImpactUniqueId];
                    if (impact != null && uniqueId != null && !string.IsNullOrEmpty(uniqueId.ToString()))
                    {
                        // impactUniqueId is already populated so we send a 409 Conflict error
                        logger.LogInformation($"This is an existing impact with uniqueImpactId: {uniqueId}, RequestId: {requestId}, CorrelationId: {correlationId}");
                        var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_ALREADY_EXISTS, "Encountered an error creating an impact. This impact already exists");
                        stopWatch.Stop();
                        logger.LogInformation($"Ending impact creation request at: {DateTime.Now.ToUniversalTime()}");
                        logger.LogInformation($"Total time taken to process request: {stopWatch.ElapsedMilliseconds} ms.");

                        return Helper.CreateResponse(HttpStatusCode.Conflict, errorRes);
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Error encountered creating impact: {ex}");
                    stopWatch.Stop();
                    logger.LogInformation($"Ending impact creation request at: {DateTime.Now.ToUniversalTime()}");
                    logger.LogInformation($"Total time taken to process request: {stopWatch.ElapsedMilliseconds} ms.");
                    var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_CREATION_ERROR, "Encountered an error creating an impact.");
                    return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorRes);
                }

                logger.LogInformation($"Unique impact id is {uniqueImpactId}, RequestId: {requestId}, CorrelationId: {correlationId}");
                if (impact != null)
                {
                    impact.Properties[Constants.ImpactUniqueId] = uniqueImpactId.ToString();
                    impact.Properties[Constants.ReportedTimeUtc] = DateTime.Now.ToUniversalTime();
                    impact.ApiVersion = apiVersion;

                    // validate if the appID is valid Guid. If it's a guid only then it will be stored in Kusto.
                    bool isValidAppId = Validator.IsValidGuid(appId);
                    if (isValidAppId)
                    {
                        // if it's guid only then store in database
                        impact.AppId = Guid.Parse(appId);
                    }

                    string response = JsonConvert.SerializeObject(impact, new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() });
                    try
                    {
                        await this.SendEvent(logger, this.impactIngestEventHubClient, impact, uniqueImpactId, tenantId, requestId, correlationId, uniqueImpactId);
                        logger.LogInformation($"Reported impact with ImpactId: {uniqueImpactId}, Request Id: {requestId} and Correlation Id: {correlationId} at {impact.Properties[Constants.ReportedTimeUtc]}");
                        logger.LogTrace("<< End: OnResourceCreationBegin()");
                        stopWatch.Stop();
                        logger.LogInformation($"Ending impact creation request at: {DateTime.Now.ToUniversalTime()}");
                        logger.LogInformation($"Total time taken to process request: {stopWatch.ElapsedMilliseconds} ms.");

                        return this.Content(response, "application/json");
                    }
                    catch (Exception ex)
                    {
                        logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Encountered an error in sending impact event to event hub. Error: {ex.StackTrace}");
                        var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_INGESTION_ERROR, "Encountered an error while sending impact.");
                        return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorRes);
                    }
                }

                var errorResponse = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_CREATION_ERROR, "Encountered an error creating an impact.");
                logger.LogTrace("<< End: OnResourceCreationBegin()");
                return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorResponse);
            }
        }

        /// <summary>
        /// This method is used for reporting workload impacts via internal api version 2023-07-01-preview.
        /// Changes include:
        /// 1. addition of impactedResourceId, impactedResourceType, region, childImpactedResources fields.
        /// 2. each resourceId specified in childImpactedResources will be stored as individual impact.
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="subscriptionId"></param>
        /// <param name="request"></param>
        /// <param name="isInternal"></param>
        /// <param name="workloadImpactName"></param>
        /// <returns></returns>
        [NonAction]
        public async Task<IActionResult> CreateAndReportInternalImpactsV3(ILogger logger, string subscriptionId, HttpRequest request, bool isInternal, string workloadImpactName)
        {
            logger.LogTrace(">>Start: CreateAndReportInternalImpactsV3()");
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            logger.LogTrace($"Received user request. Start impact creation flow at: {DateTime.Now.ToUniversalTime()}");

            // need to add this to get past the issue mentioned here https://github.com/dotnet/aspnetcore/issues/7644
            var syncIOFeature = this.HttpContext.Features.Get<IHttpBodyControlFeature>();
            if (syncIOFeature != null)
            {
                syncIOFeature.AllowSynchronousIO = true;
            }

            using (StreamReader reader = new StreamReader(request.Body))
            {
                var input = reader.ReadToEnd();
                Guid uniqueImpactId = Guid.NewGuid();
                string requestId = request.Headers[Constants.ClientRequestId].ToString();
                string correlationId = request.Headers[Constants.CorrelationId].ToString();
                string appId = request.Headers[Constants.ClientAppId].ToString();
                string apiVersion = request.Query[Constants.ApiVersion];
                string tenantId = request.Headers[Constants.TenantId].ToString();
                var result = this.ValidateInput(logger, isInternal, reader, stopWatch,
                    subscriptionId, tenantId,this.HttpContext, this.impactCategories,
                    this.allowedCategories, requestId, correlationId, apiVersion, input);
                if (result.StatusCode != (int)HttpStatusCode.OK)
                {
                    return result;
                }

                logger.LogInformation($"Creating impact from the request input at Timestamp: {DateTime.Now}, RequestId: {requestId}, CorrelationId: {correlationId}");
                Impact impact = null;
                try
                {
                    impact = Helper.GetImpact(logger, input, requestId, correlationId);

                    // TODO: add logic to identify duplicate impacts
                }
                catch (Exception ex)
                {
                    logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Error encountered creating impact: {ex}");
                    stopWatch.Stop();
                    logger.LogInformation($"Ending impact creation request at: {DateTime.Now.ToUniversalTime()}");
                    logger.LogInformation($"Total time taken to process request: {stopWatch.ElapsedMilliseconds} ms.");
                    var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_CREATION_ERROR, "Encountered an error creating an impact.");
                    return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorRes);
                }

                logger.LogInformation($"Unique impact id is {uniqueImpactId}, RequestId: {requestId}, CorrelationId: {correlationId}");

                if (impact != null)
                {
                    impact.Id = request.Headers[Constants.ImpactId].ToString(); // armId
                    impact.Name = workloadImpactName; // name
                    impact.Type = Constants.ImpactType; // impactType
                    IActionResult response;

                    // store parent impact. parentImpactId will be null.
                    try
                    {
                        response = await this.ReportImpactAsync(impact, uniqueImpactId, apiVersion, logger, appId, requestId, correlationId, tenantId, stopWatch, isInternal, null);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Encountered an error in sending impact event to event hub. Error: {ex.StackTrace}");
                        var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_INGESTION_ERROR, "Encountered an error while sending impact.");
                        return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorRes);
                    }

                    // store child resources if it exists
                    var impactedResourcesToBePopulated = impact.Properties.SelectToken(Constants.ChildImpactedResources);
                    if (impactedResourcesToBePopulated != null
                        && impactedResourcesToBePopulated.Type != JTokenType.Null
                        && impactedResourcesToBePopulated.HasValues)
                    {
                        var impactedResources = impactedResourcesToBePopulated.Values<JToken>();
                        impact.Properties.SelectToken(Constants.ChildImpactedResources).Parent.Remove();

                        // remove impactedResourceAugmentation from top level when evaluating child impacts.
                        var impactedResourceAugmentation = impact.Properties.SelectToken(Constants.ImpactedResourceAugmentation);
                        if (impactedResourceAugmentation != null
                            && impactedResourceAugmentation.Type != JTokenType.Null
                            && impactedResourceAugmentation.HasValues)
                        {
                            impact.Properties.SelectToken(Constants.ImpactedResourceAugmentation).Parent.Remove();
                        }

                        // remove region from top level when evaluating child impacts.
                        var regionInInput = impact.Properties[Constants.Region];
                        if (regionInInput != null
                            && regionInInput.Type != JTokenType.Null
                            && !string.IsNullOrEmpty(regionInInput.ToString()))
                        {
                            impact.Properties.SelectToken(Constants.Region).Parent.Remove();
                        }

                        if (impactedResources.Children().Count() > Constants.UpperLimitImpactCountV3)
                        {
                            logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, ChildImpactedResources cannot be more than 200. Please update your paylaod and try again.");
                            var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_INGESTION_ERROR, "ChildImpactedResources cannot be more than 200. Please update your paylaod and try again");
                            return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorRes);
                        }

                        foreach (var resource in impactedResources)
                        {
                            Impact impactToBeIngested = new Impact()
                            {
                                Name = workloadImpactName,
                                Id = this.Request.Headers[Constants.ImpactId].ToString(),
                                Type = Constants.ImpactType,
                                IsInternal = true,
                                ApiVersion = apiVersion,
                                Properties = impact.Properties.DeepClone(),
                            };
                            var impactId = Guid.NewGuid();
                            var resourceUri = resource[Constants.ResourceUri];
                            var impactedResourceType = resource[Constants.ImpactedResourceType];

                            if (resourceUri == null
                                || resource.Type == JTokenType.Null
                                || (resource != null
                                    && resource.Type != JTokenType.Null
                                    && string.IsNullOrEmpty(resourceUri.ToString()))
                                || (resourceUri != null && resourceUri.Type == JTokenType.Null)
                                || impactedResourceType == null
                                || impactedResourceType.Type == JTokenType.Null
                                || (impactedResourceType != null
                                    && impactedResourceType.Type != JTokenType.Null
                                    && string.IsNullOrEmpty(impactedResourceType.ToString()))
                                || (impactedResourceType != null && impactedResourceType.Type == JTokenType.Null))
                            {

                                // if either impactedResourceId or impactedResourceType is null throw 400
                                logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, ImpactedResourceId and ImpactedResourceType are mandatory fields. Either impactedResourceId: {resource[Constants.ResourceUri]} or impactedResourceType: {resource[Constants.ImpactedResourceType]} is empty or null");
                                var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.VALIDATION_ERROR, $"ImpactedResourceId and ImpactedResourceType are mandatory fields. Either impactedResourceId: {resource[Constants.ResourceUri]} or impactedResourceType: {resource[Constants.ImpactedResourceType]} is empty or null");
                                return Helper.CreateResponse(HttpStatusCode.BadRequest, errorRes);
                            }

                            impactToBeIngested.Properties[Constants.ResourceUri] = resource[Constants.ResourceUri].ToString();
                            impactToBeIngested.Properties[Constants.ImpactedResourceType] = resource[Constants.ImpactedResourceType].ToString();
                            impactToBeIngested.Properties[Constants.ImpactedResourceAugmentation] = resource[Constants.ImpactedResourceAugmentation];
                            impactToBeIngested.Properties[Constants.Region] = resource[Constants.Region];

                            // validate child impact
                            result = this.ValidateInput(logger, isInternal, reader, stopWatch,
                                            subscriptionId, tenantId, this.HttpContext, this.impactCategories,
                                            this.allowedCategories, requestId, correlationId, apiVersion, JsonConvert.SerializeObject(impactToBeIngested));
                            if (result.StatusCode != (int)HttpStatusCode.OK)
                            {
                                return result;
                            }

                            try
                            {
                                // store child impacted resource as individual impact with parentImpactId as that of parent impact.
                                await this.ReportImpactAsync(impactToBeIngested, impactId, apiVersion, logger, appId, requestId, correlationId, tenantId, stopWatch, isInternal, uniqueImpactId);
                            }
                            catch (Exception ex)
                            {
                                logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Encountered an error in sending impact event to event hub. Error: {ex.StackTrace}");
                                var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_INGESTION_ERROR, "Encountered an error while sending impact.");
                                return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorRes);
                            }
                        }
                    }

                    return response;
                }

                var errorResponse = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_CREATION_ERROR, "Encountered an error creating an impact.");
                logger.LogTrace("<< End: CreateAndReportInternalImpactsV3()");
                return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorResponse);
            }
        }

        /// <summary>
        /// This method is used for version 2023-02-01-preview versions of internal API.
        /// This version of the API support impactedResources field.
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="subscriptionId"></param>
        /// <param name="workloadImpactName"></param>
        /// <param name="httpRequest"></param>
        /// <param name="isInternal"></param>
        /// <returns></returns>
        [NonAction]
        public async Task<IActionResult> CreateAndReportInternalImpactsInitialVersion(ILogger logger, string subscriptionId, string workloadImpactName, HttpRequest httpRequest, bool isInternal)
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            logger.LogTrace($"Received user request. Start impact creation flow at: {DateTime.Now.ToUniversalTime()}");
            // need to add this to get past the issue mentioned here https://github.com/dotnet/aspnetcore/issues/7644
            var syncIOFeature = HttpContext.Features.Get<IHttpBodyControlFeature>();
            if (syncIOFeature != null)
            {
                syncIOFeature.AllowSynchronousIO = true;
            }

            using (StreamReader reader = new StreamReader(httpRequest.Body))
            {
                var input = reader.ReadToEnd();
                Guid uniqueImpactId = Guid.NewGuid();
                string requestId = httpRequest.Headers[Constants.ClientRequestId].ToString();
                string correlationId = httpRequest.Headers[Constants.CorrelationId].ToString();
                string apiVersion = httpRequest.Query[Constants.ApiVersion];
                string appId = httpRequest.Headers[Constants.ClientAppId].ToString();
                string tenantId = httpRequest.Headers[Constants.TenantId];   
                var result = this.ValidateInput(logger, isInternal, reader, stopWatch,
                    subscriptionId,tenantId, this.HttpContext, this.impactCategories,
                    this.allowedCategories, requestId, correlationId, apiVersion, input);
                if (result.StatusCode != (int)HttpStatusCode.OK)
                {
                    return result;
                }

                logger.LogInformation($"Creating impact from the request input at Timestamp: {DateTime.Now}, RequestId: {requestId}, CorrelationId: {correlationId}");
                Impact impact = null;
                try
                {
                    impact = Helper.GetImpact(logger, input, requestId, correlationId);
                    // TODO: add logic to identify duplicate impacts
                }
                catch (Exception ex)
                {
                    logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Error encountered creating impact: {ex}");
                    stopWatch.Stop();
                    logger.LogInformation($"Ending impact creation request at: {DateTime.Now.ToUniversalTime()}");
                    logger.LogInformation($"Total time taken to process request: {stopWatch.ElapsedMilliseconds} ms.");

                    var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_CREATION_ERROR, "Encountered an error creating an impact.");
                    return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorRes);
                }

                logger.LogInformation($"Unique impact id is {uniqueImpactId}, RequestId: {requestId}, CorrelationId: {correlationId}");
                if (impact != null)
                {
                    var impactedResourcesToBePopulated = impact.Properties.SelectToken(Constants.ImpactedResources);
                    var impactedResources = impactedResourcesToBePopulated.Values<JToken>();
                    impact.Properties.SelectToken(Constants.ImpactedResources).Parent.Remove();
                    foreach (var resource in impactedResources)
                    {
                        Impact impactToBeIngested = new Impact()
                        {
                            Name = workloadImpactName,
                            Id = this.Request.Headers[Constants.ImpactId].ToString(),
                            Type = Constants.ImpactType,
                            IsInternal = true,
                            ApiVersion = apiVersion,
                            Properties = impact.Properties.DeepClone(),
                        };
                        var impactId = Guid.NewGuid();
                        impactToBeIngested.Properties[Constants.ResourceUri] = resource[Constants.ArmId].ToString();
                        try
                        {
                            await this.ReportImpactAsync(impactToBeIngested, impactId, apiVersion, logger, appId, requestId, correlationId, tenantId, stopWatch, isInternal, uniqueImpactId);
                        }
                        catch (Exception ex)
                        {
                            logger.LogError($"RequestId: {requestId}, CorrelationId: {correlationId}, Encountered an error in sending impact event to event hub. Error: {ex.StackTrace}");
                            var errorRes = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_INGESTION_ERROR, "Encountered an error while sending impact.");
                            return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorRes);
                        }
                    }

                    impact.ApiVersion = apiVersion;
                    impact.Id = httpRequest.Headers[Constants.ImpactId].ToString(); // armId
                    impact.Name = workloadImpactName; // name
                    impact.Type = Constants.ImpactType; // impactType
                    impact.Properties[Constants.ReportedTimeUtc] = DateTime.Now.ToUniversalTime();
                    impact.Properties[Constants.ImpactUniqueId] = uniqueImpactId.ToString(); //parent impact ID
                    impact.Properties[Constants.ImpactedResources] = impactedResourcesToBePopulated;
                    string response = JsonConvert.SerializeObject(impact, new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() });
                    return Content(response, "application/json");
                }

                var errorResponse = Helper.CreateErrorResponse(ErrorCodeConstants.IMPACT_CREATION_ERROR, "Encountered an error creating an impact.");
                return Helper.CreateResponse(HttpStatusCode.InternalServerError, errorResponse);
            }
        }
    }
}

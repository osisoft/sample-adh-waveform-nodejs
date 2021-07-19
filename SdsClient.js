var axios = require('axios');
const { URLSearchParams } = require('url');
var zlib = require('zlib');

var logError = function (err) {
  success = false;
  errorCap = err;
  if (typeof err.statusCode !== 'undefined' && err.statusCode === 302) {
    console.log('Sds Object already present in the Service\n');
    console.trace();
  } else {
    console.trace();
    console.log(err.message);
    console.log(err.stack);
    console.log(err.options.headers['Operation-Id']);
    throw err;
  }

  console.log('Operation Id:' + err);
};
String.prototype.format = function (args) {
  var str = this;
  return str.replace(String.prototype.format.regex, function (item) {
    var intVal = parseInt(item.substring(1, item.length - 1));
    var replace;
    if (intVal >= 0) {
      replace = args[intVal];
    } else if (intVal === -1) {
      replace = '{';
    } else if (intVal === -2) {
      replace = '}';
    } else {
      replace = '';
    }
    return replace;
  });
};
String.prototype.format.regex = new RegExp('{-?[0-9]+}', 'g');

module.exports = {
  SdsClient: function (url, apiVersion) {
    this.url = url;
    this.version = 0.1;
    this.apiBase = '/api/' + apiVersion;
    this.typesBase = this.apiBase + '/Tenants/{0}/Namespaces/{1}/Types';
    this.streamsBase = this.apiBase + '/Tenants/{0}/Namespaces/{1}/Streams';
    this.streamViewsBase =
      this.apiBase + '/Tenants/{0}/Namespaces/{1}/StreamViews';
    this.insertValuesBase = '/Data';
    this.getLastValueBase = '/{0}/Data/Last';
    this.getFirstValueBase = '/{0}/Data/First';
    this.getWindowValuesBase =
      '/{0}/Data?startIndex={1}&endIndex={2}&filter={3}';
    this.getRangeValuesBase =
      '/{0}/Data/Transform?startIndex={1}&skip={2}&count={3}&reversed={4}&boundaryType={5}&streamViewId={6}';
    this.getRangeValuesInterpolatedBase =
      '/{0}/Data/Transform/Interpolated?startIndex={1}&endindex={2}&count={3}';
    this.getSamplesBase =
      '/{0}/Data/Transform/Sampled?startIndex={1}&endindex={2}&intervals={3}&sampleBy={4}&filter={5}';
    this.updateValuesBase = '/Data';
    this.replaceValuesBase = '/Data?allowCreate=false';
    this.removeSingleValueBase = '/{0}/Data?index={1}';
    this.removeMultipleValuesBase = '/{0}/Data?startIndex={1}&endIndex={2}';
    this.getTenantRoleBase = this.apiBase + '/Tenants/{0}/Roles';
    this.updateStreamAclBase = this.streamsBase + '/{2}/AccessControl';
    this.getCommunityStreamsBase =
      this.apiBase +
      '-preview/Tenants/{0}/Search/Communities/{1}/Streams?query={2}';
    this.token = '';
    this.tokenExpires = '';

    // returns an access token
    this.getToken = function (clientId, clientSecret, resource) {
      return axios({
        url: resource + '/identity/.well-known/openid-configuration',
        method: 'GET',
        headers: {
          Accept: 'application/json',
          'Accept-Encoding': 'gzip',
        },
      })
        .then(function (res) {
          var obj = res.data;
          authority = new URL(obj.token_endpoint);
          resUrl = new URL(resource);
          if (
            authority.protocol === resUrl.protocol &&
            authority.hostname === resUrl.hostname
          ) {
            var body = new URLSearchParams({
              grant_type: 'client_credentials',
              client_id: clientId,
              client_secret: clientSecret,
            });

            return axios.post(authority.href, body.toString(), {
              headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
              },
            });
          } else {
            logError(`Encountered error parsing authority: ${authority.href}`);
          }
        })
        .catch(function (err) {
          logError(err);
        });
    };

    // create a type
    this.createType = function (tenantId, namespaceId, type) {
      return axios({
        url:
          this.url +
          this.typesBase.format([tenantId, namespaceId]) +
          '/' +
          type.Id,
        method: 'POST',
        headers: this.getHeaders(),
        data: JSON.stringify(type).toString(),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // create a stream under the Sds Service
    this.createStream = function (tenantId, namespaceId, stream) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          stream.Id,
        method: 'POST',
        headers: this.getHeaders(),
        data: JSON.stringify(stream).toString(),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // get streams from the Sds Service
    this.getStreams = function (
      tenantId,
      namespaceId,
      queryString,
      skip,
      count
    ) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '?' +
          'query=' +
          queryString,
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // get stream from the Sds Service
    this.getStream = function (tenantId, namespaceId, streamId) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId,
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // get streams from the Sds Service
    this.getTypes = function (tenantId, namespaceId, queryString, skip, count) {
      return axios({
        url:
          this.url +
          this.typesBase.format([tenantId, namespaceId]) +
          '?' +
          'query=' +
          queryString +
          '&skip=' +
          skip +
          '&count=' +
          count,
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // create a streamView
    this.createStreamView = function (tenantId, namespaceId, streamView) {
      return axios({
        url:
          this.url +
          this.streamViewsBase.format([tenantId, namespaceId]) +
          '/' +
          streamView.Id,
        method: 'POST',
        headers: this.getHeaders(),
        data: JSON.stringify(streamView).toString(),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // get an SdsStreamViewMap
    this.getStreamViewMap = function (tenantId, namespaceId, streamViewId) {
      return axios({
        url:
          this.url +
          this.streamViewsBase.format([tenantId, namespaceId]) +
          '/' +
          streamViewId +
          '/Map',
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // create tags
    this.updateStreamType = function (
      tenantId,
      namespaceId,
      streamId,
      streamViewId
    ) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          '/Type?streamViewId=' +
          streamViewId,
        method: 'PUT',
        headers: this.getHeaders(),
      });
    };

    // create tags
    this.updateTags = function (tenantId, namespaceId, streamId, tags) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          '/Tags',
        method: 'PUT',
        headers: this.getHeaders(),
        data: JSON.stringify(tags),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // create metadata
    this.updateMetadata = function (tenantId, namespaceId, streamId, metadata) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          '/Metadata',
        method: 'PUT',
        headers: this.getHeaders(),
        data: JSON.stringify(metadata),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // create metadata
    this.patchMetadata = function (tenantId, namespaceId, streamId, patch) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          '/Metadata',
        method: 'PATCH',
        headers: this.getHeaders(),
        data: JSON.stringify(patch),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // get tags
    this.getTags = function (tenantId, namespaceId, streamId) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          '/Tags',
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // get metadata
    this.getMetadata = function (tenantId, namespaceId, streamId, key) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          '/Metadata/' +
          key,
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // insert an array of events
    this.insertEvents = function (tenantId, namespaceId, streamId, events) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          this.insertValuesBase,
        method: 'POST',
        headers: this.getHeaders(),
        data: JSON.stringify(events),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // get last value added to stream
    this.getLastValue = function (tenantId, namespaceId, streamId) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.getLastValueBase.format([streamId]),
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // get last value added to stream
    this.getFirstValue = function (tenantId, namespaceId, streamId) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.getFirstValueBase.format([streamId]),
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // retrieve a window of events
    this.getWindowValues = function (
      tenantId,
      namespaceId,
      streamId,
      start,
      end,
      filter = ''
    ) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.getWindowValuesBase.format([streamId, start, end, filter]),
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // retrieve a window of events in table format
    this.getWindowValuesTable = function (
      tenantId,
      namespaceId,
      streamId,
      start,
      end
    ) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.getWindowValuesBase.format([streamId, start, end, '']) +
          '&form=tableh',
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // retrieve a range of value based on boundary type
    this.getRangeValues = function (
      tenantId,
      namespaceId,
      streamId,
      start,
      skip,
      count,
      reversed,
      boundaryType,
      streamView = ''
    ) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.getRangeValuesBase.format([
            streamId,
            start,
            skip,
            count,
            reversed,
            boundaryType,
            streamView,
          ]),
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // retrieve a range of value based on boundary type
    this.getRangeValuesInterpolated = function (
      tenantId,
      namespaceId,
      streamId,
      start,
      end,
      count
    ) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.getRangeValuesInterpolatedBase.format([
            streamId,
            start,
            end,
            count,
          ]),
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // retrieve a sample from a stream
    this.getSamples = function (
      tenantId,
      namespaceId,
      streamId,
      start,
      end,
      intervals,
      sampleBy,
      filter = '',
      streamViewId = ''
    ) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.getSamplesBase.format([
            streamId,
            start,
            end,
            intervals,
            sampleBy,
            filter,
            streamViewId,
          ]),
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // update a stream
    this.updateStream = function (tenantId, namespaceId, stream) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          stream.Id,
        method: 'PUT',
        headers: this.getHeaders(),
        data: JSON.stringify(stream).toString(),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // update an array of events
    this.updateEvents = function (tenantId, namespaceId, streamId, events) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          this.updateValuesBase,
        method: 'PUT',
        headers: this.getHeaders(),
        data: JSON.stringify(events),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // replace an array of events
    this.replaceEvents = function (tenantId, namespaceId, streamId, events) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId +
          this.replaceValuesBase,
        method: 'PUT',
        headers: this.getHeaders(),
        data: JSON.stringify(events),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // delete an event
    this.deleteEvent = function (tenantId, namespaceId, streamId, index) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.removeSingleValueBase.format([streamId, index]),
        method: 'DELETE',
        headers: this.getHeaders(),
      });
    };

    // delete a window of events
    this.deleteWindowEvents = function (
      tenantId,
      namespaceId,
      streamId,
      start,
      end
    ) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          this.removeMultipleValuesBase.format([streamId, start, end]),
        method: 'DELETE',
        headers: this.getHeaders(),
      });
    };

    // delete a type
    this.deleteType = function (tenantId, namespaceId, typeId) {
      return axios({
        url:
          this.url +
          this.typesBase.format([tenantId, namespaceId]) +
          '/' +
          typeId,
        method: 'DELETE',
        headers: this.getHeaders(),
      });
    };

    // delete a stream
    this.deleteStream = function (tenantId, namespaceId, streamId) {
      return axios({
        url:
          this.url +
          this.streamsBase.format([tenantId, namespaceId]) +
          '/' +
          streamId,
        method: 'DELETE',
        headers: this.getHeaders(),
      });
    };

    // delete a StreamView
    this.deleteStreamView = function (tenantId, namespaceId, streamViewId) {
      return axios({
        url:
          this.url +
          this.streamViewsBase.format([tenantId, namespaceId]) +
          '/' +
          streamViewId,
        method: 'DELETE',
        headers: this.getHeaders(),
      });
    };

    // get tenant roles
    this.getTenantRoles = function (tenantId) {
      return axios({
        url: this.url + this.getTenantRoleBase.format([tenantId]),
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // update stream ACL
    this.patchStreamAccessControl = function (
      tenantId,
      namespaceId,
      streamId,
      patch
    ) {
      return axios({
        url:
          this.url +
          this.updateStreamAclBase.format([tenantId, namespaceId, streamId]),
        method: 'PATCH',
        headers: this.getHeaders(),
        data: JSON.stringify(patch),
        transformRequest: [(data, headers) => this.gzipCompress(data, headers)],
      });
    };

    // search for community streams
    this.getCommunityStreams = function (tenantId, communityId, query) {
      return axios({
        url:
          this.url +
          this.getCommunityStreamsBase.format([tenantId, communityId, query]),
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    // get last value from stream self link
    this.getLastValueSelf = function (self) {
      return axios({
        url: self + '/Data/Last',
        method: 'GET',
        headers: this.getHeaders(),
      });
    };

    this.gzipCompress = function (data, headers) {
      if (
        'Content-Encoding' in headers &&
        headers['Content-Encoding'].toLowerCase() === 'gzip'
      )
        return zlib.gzipSync(data);
      return data;
    };

    this.getHeaders = function () {
      return {
        'Accept-Encoding': 'gzip',
        'Content-Encoding': 'gzip',
        Authorization: 'bearer ' + this.token,
        'Content-type': 'application/json',
        Accept: '*/*; q=1',
      };
    };
  },
};

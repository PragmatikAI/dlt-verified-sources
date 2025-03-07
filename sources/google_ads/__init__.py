"""
Preliminary implementation of Google Ads pipeline.
"""

from typing import Iterator, List, Union
import dlt
import tempfile
from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials
import json
from .helpers.data_processing import to_dict
from .helpers.query import convert_schema_into_query, get_fields_from_schema, get_date_range, get_date_range_slices
from .utils.schema_helpers import ResourceSchemaLoader

from apiclient.discovery import Resource

try:
    from google.ads.googleads.client import GoogleAdsClient  # type: ignore
except ImportError:
    raise MissingDependencyException("Requests-OAuthlib", ["google-ads"])


DIMENSION_TABLES = [
    "accounts",
    "ad_group",
    "ad_group_ad",
    "ad_group_ad_label",
    "ad_group_label",
    "campaign_label",
    "click_view",
    "customer",
    "keyword_view",
    "geographic_view",
]


def get_client(
    credentials: Union[GcpOAuthCredentials, GcpServiceAccountCredentials],
    dev_token: str,
    impersonated_email: str,
) -> GoogleAdsClient:
    # generate access token for credentials if we are using OAuth2.0
    if isinstance(credentials, GcpOAuthCredentials):
        credentials.auth("https://www.googleapis.com/auth/adwords")
        conf = {
            "developer_token": dev_token,
            "use_proto_plus": True,
            **json.loads(credentials.to_native_representation()),
        }
        return GoogleAdsClient.load_from_dict(config_dict=conf)
    # use service account to authenticate if not using OAuth2.0
    else:
        # google ads client requires the key to be in a file on the disc..
        with tempfile.NamedTemporaryFile() as f:
            f.write(credentials.to_native_representation().encode())
            f.seek(0)
            return GoogleAdsClient.load_from_dict(
                config_dict={
                    "json_key_file_path": f.name,
                    "impersonated_email": impersonated_email,
                    "use_proto_plus": True,
                    "developer_token": dev_token,
                }
            )


@dlt.source(max_table_nesting=2)
def google_ads(
    credentials: Union[
        GcpOAuthCredentials, GcpServiceAccountCredentials
    ] = dlt.secrets.value,
    impersonated_email: str = dlt.secrets.value,
    dev_token: str = dlt.secrets.value,
    first_run: bool = False
) -> List[DltResource]:
    """
    Loads default tables for google ads in the database.
    :param credentials:
    :param dev_token:
    :return:
    """
    client = get_client(
        credentials=credentials,
        dev_token=dev_token,
        impersonated_email=impersonated_email,
    )
    return [
        ad_groups(client=client, first_run=first_run),
        ad_group_ad(client=client, first_run=first_run),
        campaigns(client=client, first_run=first_run),
        click_view(client=client, first_run=first_run),
        customers(client=client, first_run=first_run),
        display_keyword_view(client=client, first_run=first_run),
        keyword_view(client=client, first_run=first_run),
        change_events(client=client),
    ]


def stream_data(client: Resource, query: str, customer_id: str = dlt.secrets.value):
    ga_service = client.get_service("GoogleAdsService")
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    for batch in stream:
        for row in batch.results:
            yield to_dict(row)


@dlt.resource(write_disposition="merge", primary_key=["campaign__id", "segments__date", "segments__ad_network_type"])
def campaigns(
    client: Resource, 
    first_run: bool, 
    customer_id: str = dlt.secrets.value,
    start_date: str = dlt.secrets.value,
    conversion_window: int = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :return:
    """
    schema = ResourceSchemaLoader().get_schema("campaign")
    fields = get_fields_from_schema(schema)
    query = convert_schema_into_query(fields, "campaign", get_date_range(start_date, conversion_window, first_run))
    
    yield from stream_data(client, query, customer_id)


@dlt.resource(write_disposition="merge", primary_key=["ad_group__id", "segments__date"])
def ad_groups(
    client: Resource, 
    first_run: bool, 
    customer_id: str = dlt.secrets.value,
    start_date: str = dlt.secrets.value,
    conversion_window: int = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :param first_run:
    :param customer_id:
    :param start_date:
    :param conversion_window:
    :return:
    """
    schema = ResourceSchemaLoader().get_schema("ad_group")
    fields = get_fields_from_schema(schema)
    query = convert_schema_into_query(fields, "ad_group", get_date_range(start_date, conversion_window, first_run))
    yield from stream_data(client, query, customer_id)


@dlt.resource(write_disposition="merge", primary_key=["adGroup__id", "adGroupAd__ad__id", "segments__date"])
def ad_group_ad(
    client: Resource, 
    first_run: bool, 
    customer_id: str = dlt.secrets.value,
    start_date: str = dlt.secrets.value,
    conversion_window: int = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :param first_run:
    :param customer_id:
    :param start_date:
    :param conversion_window:
    :return:
    """
    schema = ResourceSchemaLoader().get_schema("ad_group_ad")
    fields = get_fields_from_schema(schema)
    query = convert_schema_into_query(fields, "ad_group_ad", get_date_range(start_date, conversion_window, first_run))
    yield from stream_data(client, query, customer_id)


@dlt.resource(write_disposition="merge", primary_key=["click_view__gclid", "segments__date", "segments__ad_network_type"])
def click_view(
    client: Resource, 
    first_run: bool, 
    customer_id: str = dlt.secrets.value,
    start_date: str = dlt.secrets.value,
    conversion_window: int = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :param first_run:
    :param customer_id:
    :param start_date:
    :param conversion_window:
    :return:
    """
    # Queries including ClickView must have a filter limiting the results to one day
    # So we need to get the date range slices for the query
    date_range_slices = get_date_range_slices(start_date, first_run, 90)
    schema = ResourceSchemaLoader().get_schema("click_view")
    fields = get_fields_from_schema(schema)
    for date_range_slice in date_range_slices:
        query = convert_schema_into_query(fields, "click_view", [date_range_slice])
        yield from stream_data(client, query, customer_id)

@dlt.resource(write_disposition="merge", primary_key=["customer__id", "segments__date"])
def customers(
    client: Resource,
    first_run: bool, 
    customer_id: str = dlt.secrets.value,
    start_date: str = dlt.secrets.value,
    conversion_window: int = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :param first_run:
    :param customer_id:
    :param start_date:
    :param conversion_window:
    :return:
    """
    schema = ResourceSchemaLoader().get_schema("customer")
    fields = get_fields_from_schema(schema)
    query = convert_schema_into_query(fields, "customer", get_date_range(start_date, conversion_window, first_run))
    yield from stream_data(client, query, customer_id)



@dlt.resource(write_disposition="merge", primary_key=["ad_group__id", "ad_group_criterion__criterion_id", "segments__date", "segments__ad_network_type", "segments__device"])
def display_keyword_view(
    client: Resource,
    first_run: bool,
    customer_id: str = dlt.secrets.value,
    start_date: str = dlt.secrets.value,
    conversion_window: int = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :param first_run:
    :param customer_id:
    :param start_date:
    :param conversion_window:
    :return:
    """
    schema = ResourceSchemaLoader().get_schema("display_keyword_view")
    fields = get_fields_from_schema(schema)
    query = convert_schema_into_query(fields, "display_keyword_view", get_date_range(start_date, conversion_window, first_run))
    yield from stream_data(client, query, customer_id)  


@dlt.resource(write_disposition="merge", primary_key=["ad_group__id", "ad_group_criterion__criterion_id", "segments__date"])
def keyword_view(
    client: Resource,
    first_run: bool,
    customer_id: str = dlt.secrets.value,
    start_date: str = dlt.secrets.value,
    conversion_window: int = dlt.secrets.value,
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :param first_run:
    :param customer_id:
    :param start_date:
    :param conversion_window:
    :return:
    """
    schema = ResourceSchemaLoader().get_schema("keyword_view")
    fields = get_fields_from_schema(schema)
    query = convert_schema_into_query(fields, "keyword_view", get_date_range(start_date, conversion_window, first_run))
    yield from stream_data(client, query, customer_id)



@dlt.resource(write_disposition="merge", primary_key=["change_event__change_date_time","change_event__resource_name","change_event__change_resource_type"])
def change_events(
    client: Resource, customer_id: str = dlt.secrets.value
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :return:
    """
    schema = ResourceSchemaLoader().get_schema("change_event")
    fields = get_fields_from_schema(schema)
    query = convert_schema_into_query(fields, "change_event", ['change_event.change_date_time during LAST_14_DAYS'], limit=1000)
    yield from stream_data(client, query, customer_id)


@dlt.resource(write_disposition="replace")
def customer_clients(
    client: Resource, customer_id: str = dlt.secrets.value
) -> Iterator[TDataItem]:
    """
    Dlt resource which loads dimensions.
    :param client:
    :return:
    """
    # Issues a search request using streaming.
    ga_service = client.get_service("GoogleAdsService")
    query = "SELECT customer_client.status FROM customer_client"
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    for batch in stream:
        for row in batch.results:
            yield to_dict(row.customer_client)

Command Line Flag Reference
===========================

.. note::
   This page is work in progress

``client-options``
------------------

With this option you can customize the Rally's internal client that is used to connect to the benchmark candidate.

It accepts a list of comma-separated key-value pairs. The key-value pairs are be delimited by a colon. These options are passed directly to the Elasticsearch Python client API. See `their documentation on a list of supported options <http://elasticsearch-py.readthedocs.io/en/master/api.html#elasticsearch.Elasticsearch>`_.

We support the following data types:

* Strings: Have to be enclosed in single quotes. Example: ``ca_certs:'/path/to/CA_certs'``
* Numbers: There is nothing special about numbers. Example: ``sniffer_timeout:60``
* Booleans: Specify either "true" or "false". Example: ``use_ssl:True``

In addition to the options, supported by the Elasticsearch client, it is also possible to enable HTTP compression by specifying ``compressed:true``

Default value: ``timeout:90,request_timeout:90``

.. warning::
   If you provide your own client options, the default value will not be magically merged. You have to specify all client options explicitly. The only exceptions to this rule are documented below.

Examples
~~~~~~~~

Here are a few common examples:

* Enable HTTP compression: ``--client-options="compressed:true"``
* Enable SSL (if you have Shield installed): ``--client-options="use_ssl:true,verify_certs:true"``: Note that you don't need to set ``ca_cert`` (which defines the path to the root certificates). Rally does this automatically for you.
* Enable basic authentication: ``--client-options="basic_auth_user:'user',basic_auth_password:'password'"``. Please avoid the characters ``'``, ``,`` and ``:`` in user name and password as Rally's parsing of these options is currently really simple and there is no possibility to escape characters.





Changes in Swiftly 2.00
=======================

This release was a major refactor with the internals of the library. The command line tool itself remained essentially backward compatible, with the exception of the -I option going away for delete, post, and put. See below for full details of what changed and what got added.


bin/swiftly
-----------

The command line tool itself, swiftly, really hasn't changed much in its usage. There is the exception that -I has gone away for deletes, posts, and puts. Instead, -i now supports a single dash as representing stdin and representing you want a body even when one is not normally used. So what was ``somecommand | swiftly delete -I`` is now ``somecommand | swiftly delete -i -`` and what was ``swiftly delete -Ii somefile`` is now ``swiftly delete -i somefile``.

There's a new environment variable option, SWIFTLY_CONCURRENCY, so you don't have to specify --concurrency all the time.

The auth command got a bit more information.

The get command got a new --remove-empty-files option that is pretty useful in conjunction with --sub-command. It just removes any files it stores on disk that end up empty (presumably after having gone through a sub-command filter).

The put command now supports --encrypt=KEY and the get command now supports --decrypt=KEY to use AES 256 in CBC mode via PyCrypt https://www.dlitz.net/software/pycrypto/ (perhaps more options later). You can also specify the key with the SWIFTLY_CRYPT_KEY environment variable.

Made -H a synonym for -h and made -h with no header information get treated as a request for help since those are common usage patterns.

Overall improvements to the ping command.

New 'for ... do ...' command. This will issue the command after the 'do' for each item encountered from the 'for'. For example: ``swiftly for my_container do post -hx-detect-content-type:true "<item>"`` will send a POST for every object in 'my_container' with that header, resetting their content types. See ``swiftly help for`` for more information.


swiftly.client
--------------

What was a single file is now multiple files under the swiftly.client package. This should make it much easier to maintain but does mean outside code that uses swiftly.client will have to be updated. This usually just means an update to how the Client instance is created (now use StandardClient or DirectClient with the appropriate options for each instead of just Client which was getting messy).

    * swiftly.client.client - Base class Client; to form a new concrete subclass, you would need to implement 'request' and 'get_account_hash' minimally and optionally 'reset' and 'auth'.
    * swiftly.client.directclient - The direct-cluster-access implementation of the client.
    * swiftly.client.localmemcache - Same as before, just moved out into its own file.
    * swiftly.client.manager - A useful class for managing a set of client connections.
    * swiftly.client.nulllogger - Same as before, just moved out into its own file.
    * swiftly.client.standardclient - The standard-cluster-access implementation of the client.
    * swiftly.client.utils - General utility code useful when working with Swift and Swiftly, such as 'generate_temp_url' and 'get_trans_id_time'.

More features were added too:

    * The swiftly.client.manager for one, mentioned above.
    * New decode_json option for account and container listings, allowing you to have access to the raw JSON output instead of having it decoded for you.

There were some bugs fixed along the way as well:

    * The auth caching now uses a more secure method and should no longer be prone to race conditions.
    * More Exceptions are now caught; courtesy of IAD closing my connections mid-stream so often.
    * Fixed bug with case-mismatched output headers; previously it could send out User-Agent and user-agent for example.


swiftly.cli
-----------

Again, what was a single file is now multiple files under the swiftly.cli package. This should make it much easier to maintain and add new commands.

    * swiftly.cli.cli - Contains the main entry point for the command line tool.
    * swiftly.cli.command - Base class for each command the swiftly command line tool supports.
    * swiftly.cli.context - Contextual information (command line options, client/input/output managers, etc.) passed around as needed.
    * swiftly.cli.iomanager - Manages file-like objects for input and output for the command line tool, to include calling out to sub-command shells.
    * swiftly.cli.optionparser - Extended optparse.OptionParser for use with the swiftly command line tool.
    * Each of the various commands for the swiftly command line tool each have their own file now. This includes auth, decrypt, delete, encrypt, fordo, get, head, help, ping, post, put, tempurl, and trans.

More features were added, as hinted at above:

    * Verbose output got a bit more verbose by including the headers sent.
    * New decrypt and encrypt commands.

There were some bugs fixed as well:

    * Raw output of listings is now really the raw output instead of decoded and then re-encoded JSON.


swiftly.concurrency
-------------------

This has always been a bit of work-in-progress. It has changed to capture any Exceptions and Timeouts and return that information as well as actual results. Anybody using this (which is probably nobody but Swiftly itself) will have to update accordingly.

swiftly.dencrypt
----------------

General code for the new decrypt and encrypt AES options.


swiftly.filelikeiter
--------------------

Wraps an iterable to behave as a file-like object. Based on some code I had done for Swift itself and needed for the new decrypt and encrypt options.

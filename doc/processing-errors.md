# Processing Errors

task-runner by default will "humanize" any internal errors during task processing into external errors that are suitable to return to an end user. The `-humanize-errors` flag (and its corresponding env variable) can be used to disable error humanization (i.e. `-humanize-errors=false`).

The code for humanizing errors can be found in the `humanizeError()` function.

The current errors returned when error humanization is enabled:

**file could not be imported from URL because it was not accessible**

Returned when Catalyst cannot import the file from a URL after multiple retries.

**invalid video file codec or container, check your input file against the input codec and container support matrix**

Returned when Catalyst cannot handle the file because the codec or container is not supported.

**failed to probe or open file, check your input file against the input codec and container support matrix**

Returned when Catalyst cannot probe or open the file likely because the codec or container is not supported.

**error processing catalyst callback: error saving metadata file: error marshaling file metadat: json: unsupported value: NaN**

This error will be removed once https://github.com/livepeer/task-runner/issues/106 is resolved. For now, this error is known to be returned when an image file is provided instead of a video file.

**internal error processing file**

This error is the default used when no other error can be generated for the internal error within task-runner.

TODO: Add docs on other humanized errors when they are tested.

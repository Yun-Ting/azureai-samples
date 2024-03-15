---
title: Logging with log message template in C# .NET ILogger
description: Learn about app logging provided by the Microsoft.Extensions.Logging NuGet package in C#.
---

## Logging in C# and .NET

.NET supports high performance, structured logging via the <xref:Microsoft.Extensions.Logging.ILogger> API to help monitor application behavior and diagnose issues. Logs can be written to different destinations by configuring different [logging providers](logging-providers.md). Basic logging providers are built-in and there are many third-party providers available as well.

## Get started

This first example shows the basics, but it is only suitable for a trivial console app. In the next section you see how to improve the code considering scale, performance, configuration and typical programming patterns.

:::code language="csharp" source="snippets/logging/getting-started/Program.cs":::

The preceding example:

- Creates an <xref:Microsoft.Extensions.Logging.ILoggerFactory>. The `ILoggerFactory` stores all the configuration that determines where log messages are sent. In this case, you configure the console [logging provider](logging-providers.md) so that log messages are written to the console.
- Creates an <xref:Microsoft.Extensions.Logging.ILogger> with a category named "Program". The [category](#log-category) is a `string` that is associated with each message logged
by the `ILogger` object. It's used to group log messages from the same class (or category) together when searching or filtering logs.
- Calls <xref:Microsoft.Extensions.Logging.LoggerExtensions.LogInformation%2A> to log a message at the `Information` level. The [log level](#log-level) indicates the severity of the logged event and is used to filter out less important log messages. The log entry also includes a [message template](#log-message-template) `"Hello World! Logging is {Description}."` and a key-value pair `Description = fun`. The key name (or placeholder) comes from the word inside the curly braces in the template and the value comes from the remaining method argument.

[!INCLUDE [logging-samples-browser](includes/logging-samples-browser.md)]

## Log message template

Each log API uses a message template. The message template can contain placeholders for which arguments are provided. Use names for the placeholders, not numbers. The order of placeholders, not their names, determines which parameters are used to provide their values. In the following code, the parameter names are out of sequence in the message template:

```csharp
string p1 = "param1";
string p2 = "param2";
_logger.LogInformation("Parameter values: {p2}, {p1}", p1, p2);
```

The preceding code creates a log message with the parameter values in sequence:

```text
Parameter values: param1, param2
```

> [!NOTE]
> Be mindful when using multiple placeholders within a single message template, as they're ordinal-based. The names are _not_ used to align the arguments to the placeholders.

This approach allows logging providers to implement [semantic or structured logging](https://github.com/NLog/NLog/wiki/How-to-use-structured-logging). The arguments themselves are passed to the logging system, not just the formatted message template. This enables logging providers to store the parameter values as fields. Consider the following logger method:

```csharp
_logger.LogInformation("Getting item {Id} at {RunTime}", id, DateTime.Now);
```

For example, when logging to Azure Table Storage:

- Each Azure Table entity can have `ID` and `RunTime` properties.
- Tables with properties simplify queries on logged data. For example, a query can find all logs within a particular `RunTime` range without having to parse the time out of the text message.

### Log message template formatting

Log message templates support placeholder formatting. Templates are free to specify [any valid format](../../standard/base-types/formatting-types.md) for the given type argument. For example, consider the following `Information` logger message template:

```csharp
_logger.LogInformation("Logged on {PlaceHolderName:MMMM dd, yyyy}", DateTimeOffset.UtcNow);
// Logged on January 06, 2022
```

In the preceding example, the `DateTimeOffset` instance is the type that corresponds to the `PlaceHolderName` in the logger message template. This name can be anything as the values are ordinal-based. The `MMMM dd, yyyy` format is valid for the `DateTimeOffset` type.

For more information on `DateTime` and `DateTimeOffset` formatting, see [Custom date and time format strings](../../standard/base-types/custom-date-and-time-format-strings.md).

#### Examples

The following examples show how to format a message template using the `{}` placeholder syntax. Additionally, an example of escaping the `{}` placeholder syntax is shown with its output.

```csharp
logger.LogInformation("Number: {Number}", 1);               // Number: 1
logger.LogInformation("{{Number}}: {Number}", 3);           // {Number}: 3
```

> [!TIP]
>
> - In most cases, you should use log message template formatting when logging. Use of string interpolation can cause performance issues.
> - Code analysis rule [CA2254: Template should be a static expression](../../fundamentals/code-analysis/quality-rules/ca2254.md) helps alert you to places where your log messages don't use proper formatting.
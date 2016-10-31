## Mongodb ODM

This is a port of Colin Mollenhour's "Mongodb PHP ODM":https://github.com/colinmollenhour/mongodb-php-odm

### Overview

Mongodb ODM is a simple object wrapper for the Mongo PHP driver classes which makes using Mongo in your PHP application more like ORM, but _without_ the suck.

### Design Principles

* *Don't become overly complex.* There are some features that could be implemented, but if they are not it is because they would increase the complexity too much, sacrificing elegance and usability.
* *Keep Mongo schema-less.* I like schema-less. Requiring schema's in your model definitions is not schema-less. This library let's you be schema-less.
* *Follow Mongo driver convention.* In many ways, using this library will be pretty similar to using the official driver.
* *Do not add unnecessary overhead.* Sure, it is not as fast as using the driver directly, but I'm quite sure you won't notice the difference.
* *Don't Repeat Yourself.* Very little code will be repeated while using this library.
* *Reduce the number of database requests/updates.* Prevent redundant requests and multiple updates to the same document.
* *Choose your favorite design pattern.* This library now supports using MongoOdm\Collection directly as a convenience wrapper, or choose between two ActiveRecord like patterns that resemble either the "Table Data Gateway pattern":http://www.martinfowler.com/eaaCatalog/tableDataGateway.html or the "Row Data Gateway pattern":http://www.martinfowler.com/eaaCatalog/rowDataGateway.html

### Basic Idea

The basic idea is to boil the usage of the Mongo PHP library down to three classes:
* [[MongoOdm\Database]]
* [[MongoOdm\Collection]]
* [[MongoOdm\Document]]

#### [[MongoOdm\Database]]

This class encapsulates the functionality of the Mongo connection and the Mongodb class into one since in most cases an application will interface with only one database. Interfacing with more than one database is still possible, but they will not share a single connection. Features afforded by this class:
* Configurations are described with names such as 'default'.
* Database names are never used in your code other than when supplying the configuration the first time. Easily deploy the same application on different database names.
* Lazy database connections.

#### [[MongoOdm\Collection]]

This class accomplishes the following:
* Allows query results to be accessed as an iterator of models rather than arrays when used in conjunction with MongoOdm\Document.
* Combines the functionality of MongoCollection, MongoGridFS and MongoCursor into one base class.
* Allows query building by aggregating query parameters, cursor options, requested fields, etc..
* Debugging can be made easier by using the @__toString()@ method to get a string representing the full query, again mimicking the syntax of the Mongodb shell.

#### [[MongoOdm\Document]]

This class objectifies a document (or row) in the database. Extend the base class for each of your data models, typically one per collection and enjoy some very simple CRUD operations with niceties like:
* @before_*@ and @after_*@ methods for you to implement for transparently added functionality such as updating timestamps, validation, etc..
* helpers for all of the Mongo update operations which are aggregated until the next @save()@
* aliased field names so you can use short field names in the database and long names in the code. Take it or leave it.
* auto-loading of referenced documents. E.g. @echo $post->user->email@
* lazy-loading of documents and referenced documents. E.g. the above is two requests, this would only be one: @echo $post->user->id@

See the wiki for more detail.


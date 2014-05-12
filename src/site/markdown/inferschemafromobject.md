You can use the `DatasetDescriptor.Builder#schema(Class<?> type)` method to infer a dataset schema from the instance variable fields of a Java class.

For example, the following class defines a Java object that provides access to the ID, Title, Release Date, and IMDB URL for a movie database.

```java
 package org.kitesdk.examples.data;
 /** Movie class */
 class Movie {
   private int id;
   private String title;
   private String releaseDate;
   private String imdbUrl;

   public Movie(int id, String title, String releaseDate, String imdbUrl) {
     this.id = id;
     this.title = title;
     this.releaseDate = releaseDate;
     this.imdbUrl = imdbUrl;
   }
	
   public Movie() {
     // Empty constructor for serialization purposes
   }

   public int getId() {
     return id;
   }

   public void setId (int id) {
     this.id = id;
   }

   public String getTitle() {
     return title;
   }

   public void setTitle(String title) {
     this.title = title;
   }
  
   public String getReleaseDate() {
      return releaseDate;
   }
  
   public void setReleaseDate (String releaseDate) {
     this.releaseDate = releaseDate;
   }
  
   public String getImdbUrl () {
     return imdbUrl;
   }
  
   public void setImdbUrl (String imdbUrl) {
     this.imdbUrl = imdbUrl;
   }

   public void describeMovie() {
     System.out.println(title + ", ID: " + id + ", was released on " + 
       releaseDate + ". For more info, see " + imdbUrl + ".");
   }
 }
```

***
Use the `schema(Class<?>)` builder method to create a descriptor that uses the Avro schema inferred from the `Movie` class.

```java
DatasetDescriptor movieDesc = new DatasetDescriptor.Builder()
    .schema(Movie.class)
    .build();
```

The Builder uses the field names and data types to construct an Avro schema definition, which for the `Movie` class looks like this.
***
```
{
  "type":"record",
  "name":"Movie",
  "namespace":"org.kitesdk.examples.data",
  "fields":[
    {"name":"id","type":"int"},
    {"name":"title","type":"string"},
    {"name":"releaseDate","type":"string"},
    {"name":"imdbUrl","type":"string"}
  ]
}
```
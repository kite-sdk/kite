# Adding a CDK Module

Consistency of the CDK is paramount. Please follow this guide when creating new
modules.

## Module POM Configuration

The parent pom for all modules is `com.cloudera:cdk-parent:VERSION` where
`VERSION` is whatever is currently under development. When in doubt, look at
one of the existing modules. Most of the critical pom parameters are defined
by the parent pom. Modules should _not_ define a groupId or version; they're
inherited.

All dependencies and plugins should rely on the versions and configuration set
in the parent pom. This allows us to keep all CDK modules compatible, or at
least easily track their differences. No module should ever demand a different
version of an existing dependency. For example, if the parent pom defines
version 14.0 of Guava, under no circumstances should you override that in a
module. If you need a different version of a dependency, open a JIRA to track
the potential impact to the CDK as a whole. While this may seem draconian,
the compatibility of modules is critical to users, and something we take very
seriously. New dependencies should also be added to the parent pom so it's
under dependency management. This is so all other modules that may add the same
dependency in the future see and share the same version.

In the CDK top level pom, add your new module to the `<modules/>` section so
it's included in the build.

## Site Integration

Add a link to the new module to `src/site/site.xml`. Please follow the same
documentation standards and formats of the other other modules. At the time this
is written, all documentation is written in Markdown (for better or worse). To
include content in the site, create markdown documents in the
`src/site/markdown` directory of your module. You'll also want to define a
`site.xml` file that includes navigation for the module's site. Start with the
`site.xml` file in the cdk-data module and customize from there. You should
build the entire CDK site and test your output by running `mvn site site:stage`
in the top level directory. The site is assembled from the output of all modules
and placed in the `target/staging/` directory (not the `target/site/`
directory!). This is what will be published to the Github site and what users
will see.

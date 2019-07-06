/*
 * Copyright 2015 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.camel;

public class User implements Comparable<User> {
    private final String name;
    private final Long creationDate;
    private final String favoriteColor;

    public User() {
        this("", 0L, "");
    }

    public User(String name, Long creationDate, String favoriteColor) {
        this.name = name;
        this.creationDate = creationDate;
        this.favoriteColor = favoriteColor;
    }

    public String getName() {
        return name;
    }

    public Long getCreationDate() {
        return creationDate;
    }

    public String getFavoriteColor() {
        return favoriteColor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        User user = (User) o;

        if (getName() != null ? !getName().equals(user.getName()) : user.getName() != null) return false;
        if (getCreationDate() != null ? !getCreationDate().equals(user.getCreationDate()) : user.getCreationDate() != null)
            return false;
        return !(getFavoriteColor() != null ? !getFavoriteColor().equals(user.getFavoriteColor()) : user.getFavoriteColor() != null);

    }

    @Override
    public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        result = 31 * result + (getCreationDate() != null ? getCreationDate().hashCode() : 0);
        result = 31 * result + (getFavoriteColor() != null ? getFavoriteColor().hashCode() : 0);
        return result;
    }


    @Override
    public int compareTo(User o) {
        return this.getName().compareTo(o.getName());
    }
}

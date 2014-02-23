/**
 * Copyright 2014 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.hbase;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.hbase.impl.Accessor;
import org.kitesdk.data.hbase.impl.BaseDao;
import org.kitesdk.data.hbase.impl.Dao;
import org.kitesdk.data.hbase.impl.EntityMapper;

class AccessorImpl extends Accessor {
  @Override
  public <E> EntityMapper<E> getEntityMapper(Dataset<E> dataset) {
    if (!(dataset instanceof DaoDataset)) {
      return null;
    }
    Dao<E> dao = ((DaoDataset<E>) dataset).getDao();
    if (!(dao instanceof BaseDao)) {
      return null;
    }
    return ((BaseDao<E>) dao).getEntityMapper();
  }
}

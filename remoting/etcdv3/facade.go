/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package etcdv3

import (
	"sync"
	"time"
)

import (
	"github.com/apache/dubbo-getty"
	etcdv3 "github.com/dubbogo/gost/database/kv/etcd/v3"
)

import (
	"github.com/apache/dubbo-go/common"
	"github.com/apache/dubbo-go/common/constant"
	"github.com/apache/dubbo-go/common/logger"
)

type clientFacade interface {
	Client() *etcdv3.Client
	SetClient(*etcdv3.Client)
	ClientLock() *sync.Mutex
	WaitGroup() *sync.WaitGroup //for wait group control, etcd client listener & etcd client container
	Done() chan struct{}        //for etcd client control
	RestartCallBack() bool
	common.Node
}

// HandleClientRestart keeps the connection between client and server
func HandleClientRestart(r clientFacade) {

	var (
		err       error
		failTimes int
	)

	defer r.WaitGroup().Done()
LOOP:
	for {
		select {
		case <-r.Done():
			logger.Warnf("(ETCDV3ProviderRegistry)reconnectETCDV3 goroutine exit now...")
			break LOOP
			// re-register all services
		case <-r.Client().Done():
			r.ClientLock().Lock()
			clientName := etcdv3.RegistryETCDV3Client
			timeout, _ := time.ParseDuration(r.GetUrl().GetParam(constant.REGISTRY_TIMEOUT_KEY, constant.DEFAULT_REG_TIMEOUT))
			//endpoints := r.Client().Wait;
			r.Client().Close()
			r.SetClient(nil)
			r.ClientLock().Unlock()

			// try to connect to etcd,
			failTimes = 0
			for {
				select {
				case <-r.Done():
					logger.Warnf("(ETCDV3ProviderRegistry)reconnectETCDRegistry goroutine exit now...")
					break LOOP
				case <-getty.GetTimeWheel().After(timeSecondDuration(failTimes * etcdv3.ConnDelay)): // avoid connect frequent
				}
				err = ValidateClient(
					r,
					etcdv3.WithName(clientName),
					//etcdv3.WithEndpoints(endpoints...),
					etcdv3.WithTimeout(timeout),
				)
				//logger.Infof("ETCDV3ProviderRegistry.validateETCDV3Client(etcd Addr{%s}) = error{%#v}",1,
				//	endpoints, perrors.WithStack(err))
				if err == nil && r.RestartCallBack() {
					break
				}
				failTimes++
				if etcdv3.MaxFailTimes <= failTimes {
					failTimes = etcdv3.MaxFailTimes
				}
			}
		}
	}
}

// ValidateClient validates client and sets options
func ValidateClient(container clientFacade, opts ...etcdv3.Option) error {
	lock := container.ClientLock()
	lock.Lock()
	defer lock.Unlock()

	// new Client
	if container.Client() == nil {
		//newClient := etcdv3.NewConfigClient(opts)
		//container.SetClient(newClient)

		//if err != nil {
		//	logger.Warnf("new etcd client (name{%s}, etcd addresses{%v}, timeout{%d}) = error{%v}",
		//		options.name, options.endpoints, options.timeout, err)
		//	return perrors.WithMessagef(err, "new client (address:%+v)", options.endpoints)
		//}
	}

	// Client lose connection with etcd server
	//if container.Client().rawClient == nil {
	//	newClient := etcdv3.NewConfigClient(options)
	//	container.SetClient(newClient)
		//newClient, err := etcdv3.NewClient(options.name, options.endpoints, options.timeout, options.heartbeat)
		//if err != nil {
		//	logger.Warnf("new etcd client (name{%s}, etcd addresses{%v}, timeout{%d}) = error{%v}",
		//		options.name, options.endpoints, options.timeout, err)
		//	return perrors.WithMessagef(err, "new client (address:%+v)", options.endpoints)
		//}
		//container.SetClient(newClient)
	//}

	return nil
}


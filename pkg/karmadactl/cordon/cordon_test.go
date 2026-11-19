/*
Copyright 2024 The Karmada Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cordon

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	karmadaclientset "github.com/karmada-io/karmada/pkg/generated/clientset/versioned"
	fakekarmadaclient "github.com/karmada-io/karmada/pkg/generated/clientset/versioned/fake"
	"github.com/karmada-io/karmada/pkg/karmadactl/util"
)

type testFactory struct {
	cmdutil.Factory
	client karmadaclientset.Interface
}

func (t testFactory) KarmadaClientSet() (karmadaclientset.Interface, error) {
	return t.client, nil
}

func (t testFactory) FactoryForMemberCluster(string) (cmdutil.Factory, error) {
	panic("not implemented")
}

type checkTaintCondition func(cluster *clusterv1alpha1.Cluster) error

var checkClusterCordonedCondition = func(cluster *clusterv1alpha1.Cluster) error {
	if len(cluster.Spec.Taints) == 0 {
		return fmt.Errorf("expected one noschedule taint for cluster %s, but got empty taints", cluster.GetName())
	}
	var taintFound bool
	for _, taint := range cluster.Spec.Taints {
		if taint.Key == clusterv1alpha1.TaintClusterUnscheduler && taint.Effect == corev1.TaintEffectNoSchedule {
			taintFound = true
			break
		}
	}
	if !taintFound {
		return fmt.Errorf("expected taint key and value respectively %s %s to be found, "+
			"but got %v", clusterv1alpha1.TaintClusterUnscheduler, corev1.TaintEffectNoSchedule, cluster.Spec.Taints)
	}
	return nil
}

var checkClusterUncordonedCondition = func(cluster *clusterv1alpha1.Cluster) error {
	for _, taint := range cluster.Spec.Taints {
		if taint.Key == clusterv1alpha1.TaintClusterUnscheduler && taint.Effect == corev1.TaintEffectNoSchedule {
			return fmt.Errorf("expected no noschedule taint for cluster %s, but got %v", cluster.GetName(), taint)
		}
	}
	return nil
}

func TestRunCordonOrUncordon(t *testing.T) {
	clusterName := "test-cluster"
	tests := []struct {
		name                string
		desiredCordonStatus int
		f                   util.Factory
		opts                *CommandCordonOption
		prep                func(f util.Factory, opts *CommandCordonOption, desiredCordonStatus int) error
		verify              func(util.Factory) error
		wantErr             bool
		logMsg              string
	}{
		{
			name:                "RunCordonOrUncordon_CordonUncordonedCluster_ClusterCordoned",
			desiredCordonStatus: DesiredCordon,
			f:                   testFactory{client: fakekarmadaclient.NewClientset()},
			opts:                &CommandCordonOption{ClusterName: clusterName},
			prep: func(f util.Factory, _ *CommandCordonOption, _ int) error {
				return prepClusterCreation(f, clusterName)
			},
			verify: func(f util.Factory) error {
				return verifyClusterCordoned(f, clusterName, checkClusterCordonedCondition)
			},
			wantErr: false,
			logMsg:  fmt.Sprintf("%s cluster cordoned", clusterName),
		},
		{
			name:                "RunCordonOrUncordon_CordonCordonedCluster_ClusterAlreadyCordoned",
			desiredCordonStatus: DesiredCordon,
			f:                   testFactory{client: fakekarmadaclient.NewClientset()},
			opts:                &CommandCordonOption{ClusterName: clusterName},
			prep: func(f util.Factory, opts *CommandCordonOption, desiredCordonStatus int) error {
				if err := prepClusterCreation(f, clusterName); err != nil {
					return err
				}
				if err := RunCordonOrUncordon(desiredCordonStatus, f, *opts); err != nil {
					return fmt.Errorf("failed to cordon cluster %s, got error: %v", clusterName, err)
				}
				return nil
			},
			verify:  func(util.Factory) error { return nil },
			wantErr: false,
			logMsg:  fmt.Sprintf("%s cluster already cordoned", clusterName),
		},
		{
			name:                "RunCordonOrUncordon_UncordonCordonedCluster_ClusterUncordoned",
			desiredCordonStatus: DesiredUnCordon,
			f:                   testFactory{client: fakekarmadaclient.NewClientset()},
			opts:                &CommandCordonOption{ClusterName: clusterName},
			prep: func(f util.Factory, opts *CommandCordonOption, _ int) error {
				if err := prepClusterCreation(f, clusterName); err != nil {
					return err
				}
				if err := RunCordonOrUncordon(DesiredCordon, f, *opts); err != nil {
					return fmt.Errorf("failed to cordon cluster %s, got error: %v", clusterName, err)
				}
				return nil
			},
			verify: func(f util.Factory) error {
				return verifyClusterCordoned(f, clusterName, checkClusterUncordonedCondition)
			},
			wantErr: false,
			logMsg:  fmt.Sprintf("%s cluster uncordoned", clusterName),
		},
		{
			name:                "RunCordonOrUncordon_UncordonUncordonedCluster_ClusterAlreadyUncordoned",
			desiredCordonStatus: DesiredUnCordon,
			f:                   testFactory{client: fakekarmadaclient.NewClientset()},
			opts:                &CommandCordonOption{ClusterName: clusterName},
			prep: func(f util.Factory, _ *CommandCordonOption, _ int) error {
				return prepClusterCreation(f, clusterName)
			},
			verify:  func(util.Factory) error { return nil },
			wantErr: false,
			logMsg:  fmt.Sprintf("%s cluster already uncordoned", clusterName),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.logMsg != "" {
				r, w, oldStdout, err := prepLogMsgWatcher()
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := verifyMsg(r, w, oldStdout, test.logMsg); err != nil {
						t.Error(err)
					}
				}()
			}
			if err := test.prep(test.f, test.opts, test.desiredCordonStatus); err != nil {
				t.Fatalf("failed to prep test environment, got: %v", err)
			}
			err := RunCordonOrUncordon(test.desiredCordonStatus, test.f, *test.opts)
			if err == nil && test.wantErr {
				t.Fatal("expected an error, but got none")
			}
			if err != nil && !test.wantErr {
				t.Errorf("unexpected error, got: %v", err)
			}
			if err := test.verify(test.f); err != nil {
				t.Errorf("failed to verify the cordon/uncordon, got: %v", err)
			}
		})
	}
}

// verifyClusterCordoned verifies if the cluster with the given name is cordoned
// by checking its taint condition using the provided factory and condition function.
// It returns an error if the cluster retrieval fails or the condition check fails.
func verifyClusterCordoned(f util.Factory, clusterName string, checkTaintCond checkTaintCondition) error {
	client, err := f.KarmadaClientSet()
	if err != nil {
		return err
	}
	cluster, err := client.ClusterV1alpha1().Clusters().Get(context.TODO(), clusterName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get cluster %s, got error: %v", clusterName, err)
	}
	return checkTaintCond(cluster)
}

// verifyMsg checks if the expected message is in the captured stdout output.
// Restores os.Stderr after verification and returns an error if the message is missing.
func verifyMsg(r, w *os.File, oldStdOut *os.File, msgExpected string) error {
	gotMsg, err := closeAndReadStdOut(r, w)
	if err != nil {
		return err
	}
	if !strings.Contains(gotMsg, msgExpected) {
		return fmt.Errorf("expected message %s to be in %s", msgExpected, gotMsg)
	}
	os.Stdout = oldStdOut
	return nil
}

// closeAndReadStdOut closes the writer and reads from the reader to capture stdout output.
// Returns the output as a string and an error if reading fails.
func closeAndReadStdOut(r *os.File, w *os.File) (string, error) {
	// Close the writer to finish the capture.
	w.Close()

	// Read the captured output from the pipe.
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r)
	if err != nil {
		return "", fmt.Errorf("failed to read from pipe: %v", err)
	}

	output := buf.String()
	return output, err
}

// prepLogMsgWatcher redirects os.Stdout to a pipe for capturing messages.
// Returns the pipe's reader and writer, the original os.Stdout, and an error.
func prepLogMsgWatcher() (*os.File, *os.File, *os.File, error) {
	r, w, err := watchStdOut()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to watch stdError, got: %v", err)
	}
	oldStdout := os.Stdout
	os.Stdout = w
	return r, w, oldStdout, err
}

// watchStdOut creates a pipe to capture os.Stdout. Returns the pipe's reader, writer, and an error.
func watchStdOut() (*os.File, *os.File, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create pipe: %v", err)
	}
	return r, w, err
}

// prepClusterCreation creates a new cluster with the given name using the provided factory.
// It returns an error if the cluster creation fails.
func prepClusterCreation(f util.Factory, clusterName string) error {
	client, err := f.KarmadaClientSet()
	if err != nil {
		return err
	}
	testCluster := &clusterv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName},
	}
	_, err = client.ClusterV1alpha1().Clusters().Create(context.TODO(), testCluster, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create cluster %s, got: %v", testCluster, err)
	}
	return nil
}

func TestCommandCordonOption_Validate(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		wantErr     bool
	}{
		{
			name:        "valid cluster name",
			clusterName: "member1",
			wantErr:     false,
		},
		{
			name:        "valid cluster name with hyphen",
			clusterName: "member-1",
			wantErr:     false,
		},
		{
			name:        "empty cluster name",
			clusterName: "",
			wantErr:     true,
		},
		{
			name:        "invalid cluster name with uppercase",
			clusterName: "Member1",
			wantErr:     true,
		},
		{
			name:        "invalid cluster name with underscore",
			clusterName: "member_1",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &CommandCordonOption{
				ClusterName: tt.clusterName,
			}
			if err := o.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCommandCordonOption_Complete(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantName string
		wantErr  bool
	}{
		{
			name:     "valid single arg",
			args:     []string{"member1"},
			wantName: "member1",
			wantErr:  false,
		},
		{
			name:     "empty args",
			args:     []string{},
			wantName: "",
			wantErr:  true,
		},
		{
			name:     "multiple args",
			args:     []string{"member1", "member2"},
			wantName: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &CommandCordonOption{}
			err := o.Complete(tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("Complete() error = %v, wantErr %v", err, tt.wantErr)
			}
			if o.ClusterName != tt.wantName {
				t.Errorf("Complete() ClusterName = %s, want %s", o.ClusterName, tt.wantName)
			}
		})
	}
}

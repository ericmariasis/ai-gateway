//go:build !ignore_autogenerated

// Code generated by controller-gen. DO NOT EDIT.

package v1alpha1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/gateway-api/apis/v1alpha2"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LLMAPISchema) DeepCopyInto(out *LLMAPISchema) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LLMAPISchema.
func (in *LLMAPISchema) DeepCopy() *LLMAPISchema {
	if in == nil {
		return nil
	}
	out := new(LLMAPISchema)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LLMBackend) DeepCopyInto(out *LLMBackend) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LLMBackend.
func (in *LLMBackend) DeepCopy() *LLMBackend {
	if in == nil {
		return nil
	}
	out := new(LLMBackend)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *LLMBackend) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LLMBackendList) DeepCopyInto(out *LLMBackendList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]LLMBackend, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LLMBackendList.
func (in *LLMBackendList) DeepCopy() *LLMBackendList {
	if in == nil {
		return nil
	}
	out := new(LLMBackendList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *LLMBackendList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LLMBackendLocalRef) DeepCopyInto(out *LLMBackendLocalRef) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LLMBackendLocalRef.
func (in *LLMBackendLocalRef) DeepCopy() *LLMBackendLocalRef {
	if in == nil {
		return nil
	}
	out := new(LLMBackendLocalRef)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LLMBackendSpec) DeepCopyInto(out *LLMBackendSpec) {
	*out = *in
	out.APISchema = in.APISchema
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LLMBackendSpec.
func (in *LLMBackendSpec) DeepCopy() *LLMBackendSpec {
	if in == nil {
		return nil
	}
	out := new(LLMBackendSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LLMRoute) DeepCopyInto(out *LLMRoute) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LLMRoute.
func (in *LLMRoute) DeepCopy() *LLMRoute {
	if in == nil {
		return nil
	}
	out := new(LLMRoute)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *LLMRoute) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LLMRouteList) DeepCopyInto(out *LLMRouteList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]LLMRoute, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LLMRouteList.
func (in *LLMRouteList) DeepCopy() *LLMRouteList {
	if in == nil {
		return nil
	}
	out := new(LLMRouteList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *LLMRouteList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *LLMRouteSpec) DeepCopyInto(out *LLMRouteSpec) {
	*out = *in
	out.APISchema = in.APISchema
	if in.TargetRefs != nil {
		in, out := &in.TargetRefs, &out.TargetRefs
		*out = make([]v1alpha2.LocalPolicyTargetReferenceWithSectionName, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
	if in.BackendRefs != nil {
		in, out := &in.BackendRefs, &out.BackendRefs
		*out = make([]LLMBackendLocalRef, len(*in))
		copy(*out, *in)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new LLMRouteSpec.
func (in *LLMRouteSpec) DeepCopy() *LLMRouteSpec {
	if in == nil {
		return nil
	}
	out := new(LLMRouteSpec)
	in.DeepCopyInto(out)
	return out
}

package com.fuseinfo.ber.external

trait StdClass[T] {
  def setup(params: String)
  
  def apply(input: T):AnyRef

}